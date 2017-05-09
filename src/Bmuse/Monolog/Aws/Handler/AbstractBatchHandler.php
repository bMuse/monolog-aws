<?php
/**
Copyright (c) 201& Daniel Guerrero

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

abstract class AbstractBatchHandler extends AbstractProcessingHandler
{
    private $batch_size = 1;
    private $events_queue;
    private $catch_exceptions = true;

    /**
     * @var CloudWatchLogsClient
     */
    protected $client;

    public function __construct($level = Logger::DEBUG, $bubble = true)
    {
        parent::__construct($level, $bubble);

        $this->events_queue = [];
    }

    abstract protected function writeToService(array $records);

    protected function setBatchSize($batch_size)
    {
        $this->batch_size = $batch_size;
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
    }

    /**
     * {@inheritdoc}
     */
    protected function write(array $record)
    {
        $this->events_queue[] = $record;
        $this->flush();
    }

    protected function flush($force = false)
    {
        if (count($this->events_queue) == 0)
            return; //no data so don't call to the event queue

        if (!$force && count($this->events_queue) < $this->batch_size)
            return; //we cannot write into server

        //save into aws
        try
        {
            $this->writeToService($this->events_queue);
            $this->events_queue = [];
        }
        catch(\Exception $e)
        {
            $this->events_queue = [];
            if (!$this->catch_exceptions)
                throw $e;
        }
    }
}
