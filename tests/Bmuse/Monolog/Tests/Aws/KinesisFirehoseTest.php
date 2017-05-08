<?php
namespace Bmuse\Monolog\Aws\Test;

use Aws\Result;
use Aws\MockHandler;
use Aws\CommandInterface;
use Aws\Exception\AwsException;
use Psr\Http\Message\RequestInterface;
use Aws\Firehose\FirehoseClient;
use Monolog\Logger;
use Bmuse\Monolog\Aws\Handler\KinesisFirehoseHandler;

class KinesisFirehoseTest extends \PHPUnit_Framework_TestCase
{
    protected function getClientAndMock()
    {
        $mock = new MockHandler();

        // Create a client with the mock handler.
        $client = new FirehoseClient([
            'region'  => 'us-east-1',
            'version' => 'latest',
            'handler' => $mock
        ]);

        return [$client, $mock];
    }

    /**
     * @covers Bmuse\Monolog\Aws\Handler\CloudwatchLogsHandler::close()
     */
    public function testFlush()
    {
        list($client, $mock) = $this->getClientAndMock();
        $stream_name = "stream-name";
        $record = [
            'level' => Logger::INFO,
            'timestamp' => 123,
            'formatted' => 'test',
            'extra' => [],
            'context' => [],
        ];

        //test flushing
        $mock->append(function (CommandInterface $cmd, RequestInterface $req)
            use ($stream_name, $record) {
                if ($cmd->getName() != 'PutRecordBatch')
                    return new AwsException('Call should be putRecordBatch', $cmd);

                if ($cmd->get('DeliveryStreamName') != $stream_name)
                    return new AwsException('Different DeliveryStreamName', $stream_name);

                $events = $cmd->get('Records');
                if (count($events) != 1)
                    return new AwsException('Invalid number of records', $cmd);

                return new Result([
                    'FailedPutCount' => 0,
                    'RequestResponses' => [
                        [
                            'ErrorCode' => 0,
                            'ErrorMessage' => '',
                            'RecordId' => 1,
                        ]
                    ]
                ]);
        });

        $handler = new KinesisFirehoseHandler(
            $client,
            $stream_name
        );
        $handler->setCatchExceptions(false);
        $handler->handleBatch([$record]);
        $handler->close();
    }
}
