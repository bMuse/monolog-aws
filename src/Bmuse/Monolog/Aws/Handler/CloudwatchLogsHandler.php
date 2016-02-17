<?php
/**
Copyright (c) 2016 Daniel Guerrero

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
namespace Bmuse\Monolog\Aws\Handler;

use Monolog\Logger;
use Monolog\Handler\AbstractProcessingHandler;
use Aws\CloudWatchLogs\CloudWatchLogsClient;

/**
 * Stores logs into CloudwatchLogs API
 */
class CloudwatchLogsHandler extends AbstractProcessingHandler
{
    protected $stream_name;
    protected $group_name;
    protected $batch_size;

    protected $last_sequence_token;
    protected $events_queue;
    protected $catch_exceptions = false;

    /**
     * @var CloudWatchLogsClient
     */
    protected $client;

    public function __construct(
        CloudWatchLogsClient $client,
        $group_name,
        $stream_name, $params=array()
    ) {
        $params = array_merge([
            'create_group_and_stream' => true,
            'batch_size' => 10,
            'level' => Logger::DEBUG,
            'bubble' => true
        ], $params);

        parent::__construct($params['level'], $params['bubble']);

        $this->client = $client;
        $this->group_name = $group_name;
        $this->stream_name = $stream_name;
        $this->batch_size = $params['batch_size'];
        if ($params['create_group_and_stream'])
            $this->createGroupAndStream();
        else if (!$this->getStreamLastSequenceToken())
            throw new \Exception("Stream: {$stream_name} not found");

        $this->events_queue = array();
    }

    protected function createGroupAndStream()
    {
        $this->createGroup();
        $this->createStream();
    }

    protected function createGroup()
    {
        //check if exists
        $result = $this->client->describeLogGroups([
            'logGroupNamePrefix' => $this->group_name,
        ]);
        $items = $result->get('logGroups');
        foreach($items as $item) {
            if($item['logGroupName'] == $this->group_name)
                return;
        }

        //not found so create
        $this->client->createLogGroup([
            'logGroupName' => $this->group_name
        ]);
    }

    protected function createStream()
    {
        if ($this->getStreamLastSequenceToken())
            return; //already created

        //not found so create
        $this->client->createLogStream([
            'logGroupName' => $this->group_name,
            'logStreamName' => $this->stream_name,
        ]);
    }

    protected function getStreamLastSequenceToken() {
        //check if exists
        try {
            $result = $this->client->describeLogStreams([
                'logGroupName' => $this->group_name,
                'logStreamNamePrefix' => $this->stream_name,
            ]);
            $items = $result->get('logStreams');
            foreach($items as $item) {
                if($item['logStreamName'] == $this->stream_name) {
                    $this->last_sequence_token = $item['uploadSequenceToken'];
                    return true;
                }
            }
        } catch (\Exception $e) {
            return false; //logGroupName not found for example
        }

        return false;
    }

    public function setCatchExceptions($flag)
    {
        $this->catch_exceptions = $flag;
    }

    /**
     * {@inheritdoc}
     */
    public function close()
    {
        $this->flush(true);

        $this->group_name = null;
        $this->stream_name = null;
    }

    /**
     * {@inheritdoc}
     */
    protected function write(array $record)
    {
        $timestamp = null;
        if (isset($record['datetime']))
        {
            if ($record['datetime'] instanceof \DateTime)
                $timestamp = $record['datetime']->getTimestamp();
            else if (ctype_digit($record['datetime']))
                $timestamp = $record['datetime'];
        }

        if (!$timestamp)
            $timestamp = time();

        $this->events_queue[] = array(
            'timestamp' => $timestamp*1000, //we need in milliseconds
            'message' => (string) $record['formatted'],
        );
        $this->flush();
    }

    protected function flush($force = false)
    {
        if (!$this->stream_name
            || !$this->group_name)
            return; //invalid state

        if (!$force && count($this->events_queue) < $this->batch_size)
            return; //we cannot write into server

        //save into aws
        try
        {
            $params = array(
                'logGroupName' => $this->group_name,
                'logStreamName' => $this->stream_name,
                'logEvents' => $this->events_queue,

            );
            if ($this->last_sequence_token)
                $params['sequenceToken'] = $this->last_sequence_token;
            $result = $this->client->putLogEvents($params);

            $this->events_queue = array();
            $this->last_sequence_token = $result->get('nextSequenceToken');
        }
        catch(\Exception $e)
        {
            throw $e;
            $this->events_queue = array();
            $this->last_sequence_token = null;
            if ($this->catch_exceptions)
                throw $e;
        }
    }
}
