<?php

namespace MongoDB\Tests\Operation;

use Closure;
use Iterator;
use MongoDB\BSON\TimestampInterface;
use MongoDB\ChangeStream;
use MongoDB\Driver\Cursor;
use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Exception\ConnectionTimeoutException;
use MongoDB\Driver\Exception\LogicException;
use MongoDB\Driver\Exception\ServerException;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Monitoring\CommandSucceededEvent;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;
use MongoDB\Exception\ResumeTokenException;
use MongoDB\Operation\DatabaseCommand;
use MongoDB\Operation\InsertOne;
use MongoDB\Operation\Watch;
use MongoDB\Tests\CommandObserver;
use PHPUnit\Framework\ExpectationFailedException;
use ReflectionClass;
use stdClass;
use Symfony\Bridge\PhpUnit\SetUpTearDownTrait;
use function array_diff_key;
use function array_map;
use function bin2hex;
use function microtime;
use function MongoDB\server_supports_feature;
use function sprintf;
use function version_compare;

class WatchFunctionalTest extends FunctionalTestCase
{
    use SetUpTearDownTrait;

    const INTERRUPTED = 11601;
    const NOT_MASTER = 10107;

    /** @var integer */
    private static $wireVersionForStartAtOperationTime = 7;

    /** @var array */
    private $defaultOptions = ['maxAwaitTimeMS' => 500];

    private function doSetUp()
    {
        parent::setUp();

        $this->skipIfChangeStreamIsNotSupported();
        $this->createCollection();
    }

    /**
     * Prose test: "ChangeStream must continuously track the last seen
     * resumeToken"
     */
    public function testGetResumeToken()
    {
        if ($this->isPostBatchResumeTokenSupported()) {
            $this->markTestSkipped('postBatchResumeToken is supported');
        }

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $changeStream->rewind();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->getResumeToken());

        $this->insertDocument(['x' => 1]);
        $this->insertDocument(['x' => 2]);

        $this->advanceCursorUntilValid($changeStream);
        $this->assertSameDocument($changeStream->current()->_id, $changeStream->getResumeToken());

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSameDocument($changeStream->current()->_id, $changeStream->getResumeToken());

        $this->insertDocument(['x' => 3]);

        $this->advanceCursorUntilValid($changeStream);
        $this->assertSameDocument($changeStream->current()->_id, $changeStream->getResumeToken());
    }

    /**
     * Prose test: "ChangeStream must continuously track the last seen
     * resumeToken"
     */
    public function testGetResumeTokenWithPostBatchResumeToken()
    {
        if (! $this->isPostBatchResumeTokenSupported()) {
            $this->markTestSkipped('postBatchResumeToken is not supported');
        }

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);

        $events = [];

        (new CommandObserver())->observe(
            function () use ($operation, &$changeStream) {
                $changeStream = $operation->execute($this->getPrimaryServer());
            },
            function (array $event) use (&$events) {
                $events[] = $event;
            }
        );

        $this->assertCount(1, $events);
        $this->assertSame('aggregate', $events[0]['started']->getCommandName());
        $postBatchResumeToken = $this->getPostBatchResumeTokenFromReply($events[0]['succeeded']->getReply());

        $changeStream->rewind();
        $this->assertFalse($changeStream->valid());
        $this->assertSameDocument($postBatchResumeToken, $changeStream->getResumeToken());

        $this->insertDocument(['x' => 1]);
        $this->insertDocument(['x' => 2]);

        $lastEvent = null;

        (new CommandObserver())->observe(
            function () use ($changeStream) {
                $this->advanceCursorUntilValid($changeStream);
            },
            function (array $event) use (&$lastEvent) {
                $lastEvent = $event;
            }
        );

        $this->assertNotNull($lastEvent);
        $this->assertSame('getMore', $lastEvent['started']->getCommandName());
        $postBatchResumeToken = $this->getPostBatchResumeTokenFromReply($lastEvent['succeeded']->getReply());

        $this->assertSameDocument($changeStream->current()->_id, $changeStream->getResumeToken());

        $changeStream->next();
        $this->assertSameDocument($postBatchResumeToken, $changeStream->getResumeToken());
    }

    /**
     * Prose test: "ChangeStream will resume after a killCursors command is
     * issued for its child cursor."
     */
    public function testNextResumesAfterCursorNotFound()
    {
        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $changeStream->rewind();
        $this->assertFalse($changeStream->valid());

        $this->insertDocument(['_id' => 1, 'x' => 'foo']);

        $this->advanceCursorUntilValid($changeStream);

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 1, 'x' => 'foo'],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 1],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        $this->killChangeStreamCursor($changeStream);

        $this->insertDocument(['_id' => 2, 'x' => 'bar']);

        $this->advanceCursorUntilValid($changeStream);

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 2, 'x' => 'bar'],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 2],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());
    }

    public function testNextResumesAfterConnectionException()
    {
        /* In order to trigger a dropped connection, we'll use a new client with
         * a socket timeout that is less than the change stream's maxAwaitTimeMS
         * option. */
        $manager = new Manager(static::getUri(), ['socketTimeoutMS' => 50]);
        $primaryServer = $manager->selectServer(new ReadPreference(ReadPreference::RP_PRIMARY));

        $operation = new Watch($manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($primaryServer);
        $changeStream->rewind();

        $commands = [];

        (new CommandObserver())->observe(
            function () use ($changeStream) {
                $changeStream->next();
            },
            function (array $event) use (&$commands) {
                $commands[] = $event['started']->getCommandName();
            }
        );

        $expectedCommands = [
            /* The initial aggregate command for change streams returns a cursor
             * envelope with an empty initial batch, since there are no changes
             * to report at the moment the change stream is created. Therefore,
             * we expect a getMore to be issued when we first advance the change
             * stream with next(). */
            'getMore',
            /* Since socketTimeoutMS is less than maxAwaitTimeMS, the previous
             * getMore command encounters a client socket timeout and leaves the
             * cursor open on the server. ChangeStream should catch this error
             * and resume by issuing a new aggregate command. */
            'aggregate',
            /* When ChangeStream resumes, it overwrites its original cursor with
             * the new cursor resulting from the last aggregate command. This
             * removes the last reference to the old cursor, which causes the
             * driver to kill it (via mongoc_cursor_destroy()). */
            'killCursors',
        ];

        $this->assertSame($expectedCommands, $commands);
    }

    public function testResumeBeforeReceivingAnyResultsIncludesPostBatchResumeToken()
    {
        if (! $this->isPostBatchResumeTokenSupported()) {
            $this->markTestSkipped('postBatchResumeToken is not supported');
        }

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);

        $events = [];

        (new CommandObserver())->observe(
            function () use ($operation, &$changeStream) {
                $changeStream = $operation->execute($this->getPrimaryServer());
            },
            function (array $event) use (&$events) {
                $events[] = $event;
            }
        );

        $this->assertCount(1, $events);
        $this->assertSame('aggregate', $events[0]['started']->getCommandName());
        $postBatchResumeToken = $this->getPostBatchResumeTokenFromReply($events[0]['succeeded']->getReply());

        $this->assertFalse($changeStream->valid());
        $this->killChangeStreamCursor($changeStream);

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });

        $events = [];

        (new CommandObserver())->observe(
            function () use ($changeStream) {
                $changeStream->next();
            },
            function (array $event) use (&$events) {
                $events[] = $event;
            }
        );

        $this->assertCount(3, $events);

        $this->assertSame('getMore', $events[0]['started']->getCommandName());
        $this->arrayHasKey('failed', $events[0]);

        $this->assertSame('aggregate', $events[1]['started']->getCommandName());
        $this->assertResumeAfter($postBatchResumeToken, $events[1]['started']->getCommand());
        $this->arrayHasKey('succeeded', $events[1]);

        // Original cursor is freed immediately after the change stream resumes
        $this->assertSame('killCursors', $events[2]['started']->getCommandName());
        $this->arrayHasKey('succeeded', $events[2]);

        $this->assertFalse($changeStream->valid());
    }

    private function assertResumeAfter($expectedResumeToken, stdClass $command)
    {
        $this->assertObjectHasAttribute('pipeline', $command);
        $this->assertIsArray($command->pipeline);
        $this->assertArrayHasKey(0, $command->pipeline);
        $this->assertObjectHasAttribute('$changeStream', $command->pipeline[0]);
        $this->assertObjectHasAttribute('resumeAfter', $command->pipeline[0]->{'$changeStream'});
        $this->assertEquals($expectedResumeToken, $command->pipeline[0]->{'$changeStream'}->resumeAfter);
    }

    /**
     * Prose test: "$changeStream stage for ChangeStream against a server >=4.0
     * and <4.0.7 that has not received any results yet MUST include a
     * startAtOperationTime option when resuming a changestream."
     */
    public function testResumeBeforeReceivingAnyResultsIncludesStartAtOperationTime()
    {
        if (! $this->isStartAtOperationTimeSupported()) {
            $this->markTestSkipped('startAtOperationTime is not supported');
        }

        if ($this->isPostBatchResumeTokenSupported()) {
            $this->markTestSkipped('postBatchResumeToken takes precedence over startAtOperationTime');
        }

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);

        $events = [];

        (new CommandObserver())->observe(
            function () use ($operation, &$changeStream) {
                $changeStream = $operation->execute($this->getPrimaryServer());
            },
            function (array $event) use (&$events) {
                $events[] = $event;
            }
        );

        $this->assertCount(1, $events);
        $this->assertSame('aggregate', $events[0]['started']->getCommandName());
        $reply = $events[0]['succeeded']->getReply();
        $this->assertObjectHasAttribute('operationTime', $reply);
        $operationTime = $reply->operationTime;
        $this->assertInstanceOf(TimestampInterface::class, $operationTime);

        $this->assertFalse($changeStream->valid());
        $this->killChangeStreamCursor($changeStream);

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });

        $events = [];

        (new CommandObserver())->observe(
            function () use ($changeStream) {
                $changeStream->next();
            },
            function (array $event) use (&$events) {
                $events[] = $event;
            }
        );

        $this->assertCount(3, $events);

        $this->assertSame('getMore', $events[0]['started']->getCommandName());
        $this->arrayHasKey('failed', $events[0]);

        $this->assertSame('aggregate', $events[1]['started']->getCommandName());
        $this->assertStartAtOperationTime($operationTime, $events[1]['started']->getCommand());
        $this->arrayHasKey('succeeded', $events[1]);

        // Original cursor is freed immediately after the change stream resumes
        $this->assertSame('killCursors', $events[2]['started']->getCommandName());
        $this->arrayHasKey('succeeded', $events[2]);

        $this->assertFalse($changeStream->valid());
    }

    private function assertStartAtOperationTime(TimestampInterface $expectedOperationTime, stdClass $command)
    {
        $this->assertObjectHasAttribute('pipeline', $command);
        $this->assertIsArray($command->pipeline);
        $this->assertArrayHasKey(0, $command->pipeline);
        $this->assertObjectHasAttribute('$changeStream', $command->pipeline[0]);
        $this->assertObjectHasAttribute('startAtOperationTime', $command->pipeline[0]->{'$changeStream'});
        $this->assertEquals($expectedOperationTime, $command->pipeline[0]->{'$changeStream'}->startAtOperationTime);
    }

    public function testRewindMultipleTimesWithResults()
    {
        $this->skipIfIsShardedCluster('Cursor needs to be advanced multiple times and can\'t be rewound afterwards.');

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $this->insertDocument(['x' => 1]);
        $this->insertDocument(['x' => 2]);

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        // Subsequent rewind does not change iterator state
        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(0, $changeStream->key());
        $this->assertNotNull($changeStream->current());

        /* Rewinding when the iterator is still at its first element is a NOP.
         * Note: PHPLIB-448 may see rewind() throw after any call to next() */
        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertTrue($changeStream->valid());
        $this->assertSame(0, $changeStream->key());
        $this->assertNotNull($changeStream->current());

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(1, $changeStream->key());
        $this->assertNotNull($changeStream->current());

        // Rewinding after advancing the iterator is an error
        $this->expectException(LogicException::class);
        $changeStream->rewind();
    }

    public function testRewindMultipleTimesWithNoResults()
    {
        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        // Subsequent rewind does not change iterator state
        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        /* Rewinding when the iterator hasn't advanced to an element is a NOP.
         * Note: PHPLIB-448 may see rewind() throw after any call to next() */
        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());
    }

    public function testNoChangeAfterResumeBeforeInsert()
    {
        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());

        $this->insertDocument(['_id' => 1, 'x' => 'foo']);

        $this->advanceCursorUntilValid($changeStream);

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 1, 'x' => 'foo'],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 1],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        $this->killChangeStreamCursor($changeStream);

        $changeStream->next();
        $this->assertFalse($changeStream->valid());

        $this->insertDocument(['_id' => 2, 'x' => 'bar']);

        $this->advanceCursorUntilValid($changeStream);

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 2, 'x' => 'bar'],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 2],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());
    }

    public function testResumeMultipleTimesInSuccession()
    {
        $this->skipIfIsShardedCluster('getMore may return empty response before periodicNoopIntervalSecs on sharded clusters.');

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        /* Killing the cursor when there are no results will test that neither
         * the initial rewind() nor a resume attempt via next() increment the
         * key. */
        $this->killChangeStreamCursor($changeStream);

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        // A consecutive resume attempt should still not increment the key
        $this->killChangeStreamCursor($changeStream);

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());
        $this->assertNull($changeStream->current());

        /* Insert a document and advance the change stream to ensure we capture
         * a resume token. This is necessary when startAtOperationTime is not
         * supported (i.e. 3.6 server version). */
        $this->insertDocument(['_id' => 1]);

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(0, $changeStream->key());

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 1],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 1],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        /* Insert another document and kill the cursor. ChangeStream::next()
         * should resume and pick up the last insert. */
        $this->insertDocument(['_id' => 2]);
        $this->killChangeStreamCursor($changeStream);

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(1, $changeStream->key());

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 2],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 2],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        /* Insert another document and kill the cursor. It is technically
         * permissable to call ChangeStream::rewind() since the previous call to
         * next() will have left the cursor positioned at its first and only
         * result. Assert that rewind() does not execute a getMore nor does it
         * modify the iterator's state.
         *
         * Note: PHPLIB-448 may require rewind() to throw an exception here. */
        $this->insertDocument(['_id' => 3]);
        $this->killChangeStreamCursor($changeStream);

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertTrue($changeStream->valid());
        $this->assertSame(1, $changeStream->key());
        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        // ChangeStream::next() should resume and pick up the last insert
        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(2, $changeStream->key());

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 3],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 3],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());

        // Test one final, consecutive resume via ChangeStream::next()
        $this->insertDocument(['_id' => 4]);
        $this->killChangeStreamCursor($changeStream);

        $changeStream->next();
        $this->assertTrue($changeStream->valid());
        $this->assertSame(3, $changeStream->key());

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'operationType' => 'insert',
            'fullDocument' => ['_id' => 4],
            'ns' => ['db' => $this->getDatabaseName(), 'coll' => $this->getCollectionName()],
            'documentKey' => ['_id' => 4],
        ];

        $this->assertMatchesDocument($expectedResult, $changeStream->current());
    }

    public function testKey()
    {
        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), [], $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());

        $this->assertNoCommandExecuted(function () use ($changeStream) {
            $changeStream->rewind();
        });
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());

        $this->insertDocument(['_id' => 1, 'x' => 'foo']);

        $this->advanceCursorUntilValid($changeStream);
        $this->assertSame(0, $changeStream->key());

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());

        $this->killChangeStreamCursor($changeStream);

        $changeStream->next();
        $this->assertFalse($changeStream->valid());
        $this->assertNull($changeStream->key());

        $this->insertDocument(['_id' => 2, 'x' => 'bar']);

        $this->advanceCursorUntilValid($changeStream);
        $this->assertSame(1, $changeStream->key());
    }

    public function testNonEmptyPipeline()
    {
        $pipeline = [['$project' => ['foo' => [0]]]];

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), $pipeline, $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $this->insertDocument(['_id' => 1]);

        $changeStream->rewind();
        $this->assertFalse($changeStream->valid());

        $this->advanceCursorUntilValid($changeStream);

        $expectedResult = [
            '_id' => $changeStream->current()->_id,
            'foo' => [0],
        ];

        $this->assertSameDocument($expectedResult, $changeStream->current());
    }

    /**
     * Prose test: "Ensure that a cursor returned from an aggregate command with
     * a cursor id and an initial empty batch is not closed on the driver side."
     */
    public function testInitialCursorIsNotClosed()
    {
        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), []);
        $changeStream = $operation->execute($this->getPrimaryServer());

        /* The spec requests that we assert that the cursor returned from the
         * aggregate command is not closed on the driver side. We will verify
         * this by checking that the cursor ID is non-zero and that libmongoc
         * reports the cursor as alive. While the cursor ID is easily accessed
         * through ChangeStream, we'll need to use reflection to access the
         * internal Cursor and call isDead(). */
        $this->assertNotEquals('0', (string) $changeStream->getCursorId());

        $rc = new ReflectionClass(ChangeStream::class);
        $rp = $rc->getProperty('iterator');
        $rp->setAccessible(true);

        $iterator = $rp->getValue($changeStream);

        $this->assertInstanceOf('IteratorIterator', $iterator);

        $cursor = $iterator->getInnerIterator();

        $this->assertInstanceOf(Cursor::class, $cursor);
        $this->assertFalse($cursor->isDead());
    }

    /**
     * Prose test: "ChangeStream will not attempt to resume after encountering
     * error code 11601 (Interrupted), 136 (CappedPositionLost), or 237
     * (CursorKilled) while executing a getMore command."
     *
     * @dataProvider provideNonResumableErrorCodes
     */
    public function testNonResumableErrorCodes($errorCode)
    {
        $this->configureFailPoint([
            'configureFailPoint' => 'failCommand',
            'mode' => ['times' => 1],
            'data' => ['failCommands' => ['getMore'], 'errorCode' => $errorCode],
        ]);

        $this->insertDocument(['x' => 1]);

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), []);
        $changeStream = $operation->execute($this->getPrimaryServer());
        $changeStream->rewind();

        $this->expectException(ServerException::class);
        $this->expectExceptionCode($errorCode);
        $changeStream->next();
    }

    public function provideNonResumableErrorCodes()
    {
        return [
            'CappedPositionLost' => [136],
            'CursorKilled' => [237],
            'Interrupted' => [11601],
        ];
    }

    /**
     * Prose test: "ChangeStream will throw an exception if the server response
     * is missing the resume token (if wire version is < 8, this is a driver-
     * side error; for 8+, this is a server-side error)"
     */
    public function testResumeTokenNotFoundClientSideError()
    {
        if (version_compare($this->getServerVersion(), '4.1.8', '>=')) {
            $this->markTestSkipped('Server rejects change streams that modify resume token (SERVER-37786)');
        }

        $pipeline =  [['$project' => ['_id' => 0 ]]];

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), $pipeline, $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $changeStream->rewind();

        /* Insert two documents to ensure the client does not ignore the first
         * document's resume token in favor of a postBatchResumeToken */
        $this->insertDocument(['x' => 1]);
        $this->insertDocument(['x' => 2]);

        $this->expectException(ResumeTokenException::class);
        $this->expectExceptionMessage('Resume token not found in change document');
        $this->advanceCursorUntilValid($changeStream);
    }

    /**
     * Prose test: "ChangeStream will throw an exception if the server response
     * is missing the resume token (if wire version is < 8, this is a driver-
     * side error; for 8+, this is a server-side error)"
     */
    public function testResumeTokenNotFoundServerSideError()
    {
        if (version_compare($this->getServerVersion(), '4.1.8', '<')) {
            $this->markTestSkipped('Server does not reject change streams that modify resume token');
        }

        $pipeline =  [['$project' => ['_id' => 0 ]]];

        $operation = new Watch($this->manager, $this->getDatabaseName(), $this->getCollectionName(), $pipeline, $this->defaultOptions);
        $changeStream = $operation->execute($this->getPrimaryServer());

        $changeStream->rewind();
        $this->insertDocument(['x' => 1]);

        $this->expectException(ServerException::class);
        $this->advanceCursorUntilValid($changeStream);
    }

    private function assertNoCommandExecuted(callable $callable)
    {
        $commands = [];

        (new CommandObserver())->observe(
            $callable,
            function (array $event) use (&$commands) {
                $this->fail(sprintf('"%s" command was executed', $event['started']->getCommandName()));
            }
        );

        $this->assertEmpty($commands);
    }

    private function getPostBatchResumeTokenFromReply(stdClass $reply)
    {
        $this->assertObjectHasAttribute('cursor', $reply);
        $this->assertIsObject($reply->cursor);
        $this->assertObjectHasAttribute('postBatchResumeToken', $reply->cursor);
        $this->assertIsObject($reply->cursor->postBatchResumeToken);

        return $reply->cursor->postBatchResumeToken;
    }

    private function insertDocument($document)
    {
        $insertOne = new InsertOne(
            $this->getDatabaseName(),
            $this->getCollectionName(),
            $document,
            ['writeConcern' => new WriteConcern(WriteConcern::MAJORITY)]
        );
        $writeResult = $insertOne->execute($this->getPrimaryServer());
        $this->assertEquals(1, $writeResult->getInsertedCount());
    }

    private function isPostBatchResumeTokenSupported()
    {
        return version_compare($this->getServerVersion(), '4.0.7', '>=');
    }

    private function isStartAtOperationTimeSupported()
    {
        return server_supports_feature($this->getPrimaryServer(), self::$wireVersionForStartAtOperationTime);
    }

    private function killChangeStreamCursor(ChangeStream $changeStream)
    {
        $command = [
            'killCursors' => $this->getCollectionName(),
            'cursors' => [ $changeStream->getCursorId() ],
        ];

        $operation = new DatabaseCommand($this->getDatabaseName(), $command);
        $operation->execute($this->getPrimaryServer());
    }

    private function advanceCursorUntilValid(Iterator $iterator, $limitOnShardedClusters = 5)
    {
        if (! $this->isShardedCluster()) {
            $iterator->next();
            $this->assertTrue($iterator->valid());

            return;
        }

        for ($i = 0; $i < $limitOnShardedClusters; $i++) {
            $iterator->next();
            if ($iterator->valid()) {
                return;
            }
        }

        throw new ExpectationFailedException(sprintf('Expected cursor to return an element but none was found after %d attempts.', $limitOnShardedClusters));
    }

    private function skipIfIsShardedCluster($message)
    {
        if (! $this->isShardedCluster()) {
            return;
        }

        $this->markTestSkipped(sprintf('Test does not apply on sharded clusters: %s', $message));
    }
}
