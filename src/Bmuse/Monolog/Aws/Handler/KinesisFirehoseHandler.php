<?php
/**
Copyright (c) 2017 Daniel Guerrero

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
use Aws\Firehose\FirehoseClient;

/**
 * Stores logs on firehose
 */
class KinesisFirehoseHandler extends AbstractBatchHandler
{
    protected $delivery_stream;

    /**
     * @var CloudWatchLogsClient
     */
    protected $client;

    public function __construct(
        FirehoseClient $client,
        $delivery_stream,
        $level = Logger::DEBUG,
        $bubble = true
    ) {
        parent::__construct($level, $bubble);

        $this->client = $client;
        $this->delivery_stream = $delivery_stream;
        $this->setBatchSize(100);
    }

    protected function writeToService(array $records)
    {
        $records_formatted = array_map(function($item) {
            return ['Data' => $item['formatted']];
        }, $records);
        $params = [
            'DeliveryStreamName' => $this->delivery_stream,
            'Records' => $records_formatted,
        ];

        $this->client->putRecordBatch($params);
    }
}
