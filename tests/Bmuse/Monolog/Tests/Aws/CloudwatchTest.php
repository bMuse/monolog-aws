<?php
namespace Bmuse\Monolog\Aws\Test;

use Aws\Result;
use Aws\MockHandler;
use Aws\CommandInterface;
use Aws\Exception\AwsException;
use Psr\Http\Message\RequestInterface;
use Aws\CloudWatchLogs\CloudWatchLogsClient;
use Monolog\Logger;
use Bmuse\Monolog\Aws\Handler\CloudwatchLogsHandler;

class CloudwatchTest extends \PHPUnit_Framework_TestCase
{
    protected function getClientAndMock()
    {
        $mock = new MockHandler();

        // Create a client with the mock handler.
        $client = new CloudwatchLogsClient([
            'region'  => 'us-east-1',
            'version' => 'latest',
            'handler' => $mock
        ]);

        return [$client, $mock];
    }

    /**
     * @covers Bmuse\Monolog\Aws\Handler\CloudwatchLogsHandler::__construct()
     */
    public function testCreate()
    {
        list($client, $mock) = $this->getClientAndMock();
        $group_name = "group-name";
        $stream_name = "stream-name";

        $mock->append(new Result([
            'logStreams' => [
                [
                    'logStreamName' => $stream_name,
                    'uploadSequenceToken' => 'test',
                ]
            ]
        ]));

        $handler = new CloudwatchLogsHandler(
            $client,
            $group_name,
            $stream_name,
            [
                'create_group_and_stream' => false,
            ]
        );
    }

    /**
     * @covers Bmuse\Monolog\Aws\Handler\CloudwatchLogsHandler::__construct()
     */
    public function testCreateLogStreamNotFound()
    {
        list($client, $mock) = $this->getClientAndMock();
        $group_name = "group-name";
        $stream_name = "stream-name";

        $mock->append(new Result([
            'logStreams' => [
                [
                    'logStreamName' => 'invalid',
                    'uploadSequenceToken' => 'test',
                ]
            ]
        ]));

        $this->setExpectedException(\Exception::class);
        $handler = new CloudwatchLogsHandler(
            $client,
            $group_name,
            $stream_name,
            [
                'create_group_and_stream' => false,
            ]
        );
    }

    /**
     * @covers Bmuse\Monolog\Aws\Handler\CloudwatchLogsHandler::close()
     */
    public function testFlush()
    {
        list($client, $mock) = $this->getClientAndMock();
        $group_name = "group-name";
        $stream_name = "stream-name";
        $upload_sequence_token = 'sequence-token';
        $record = [
            'level' => Logger::INFO,
            'timestamp' => 123,
            'formatted' => 'test',
            'extra' => [],
            'context' => [],
        ];

        $mock->append(new Result([
            'logStreams' => [
                [
                    'logStreamName' => $stream_name,
                    'uploadSequenceToken' => $upload_sequence_token,
                ]
            ]
        ]));

        //test flushing
        $mock->append(function (CommandInterface $cmd, RequestInterface $req)
            use ($group_name, $stream_name, $upload_sequence_token, $record) {
                if ($cmd->getName() != 'PutLogEvents')
                    return new AwsException('Call should be putLogEvents', $cmd);

                if ($cmd->get('logGroupName') != $group_name)
                    return new AwsException('Different logGroupName', $cmd);

                if ($cmd->get('logStreamName') != $stream_name)
                    return new AwsException('Different logGroupName', $cmd);

                if ($cmd->get('sequenceToken') != $upload_sequence_token)
                    return new AwsException('Different sequenceToken', $cmd);

                $events = $cmd->get('logEvents');
                if (count($events) != 1)
                    return new AwsException('Invalid number of records', $cmd);

                if (!isset($events[0]['timestamp']))
                    return new AwsException('Invalid record (timestamp)', $cmd);

                if (!isset($events[0]['message']))
                    return new AwsException('Invalid record (message)', $cmd);

                return new Result(['nextSequenceToken' => 'next']);
        });

        $handler = new CloudwatchLogsHandler(
            $client,
            $group_name,
            $stream_name,
            [
                'create_group_and_stream' => false,
            ]
        );
        $handler->setCatchExceptions(false);
        $handler->handleBatch([$record]);
        $handler->close();
    }
}
