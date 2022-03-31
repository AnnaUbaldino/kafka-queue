<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use League\Flysystem\Exception;

class KafkaQueue extends Queue implements QueueContract
{

    protected $consumer, $producer;

    /**
     * KafkaQueue constructor.
     * @param $consumer
     * @param $producer
     */
    public function __construct($consumer, $producer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    /**
     * Get the size of the queue.
     *
     * @param  string|null $queue
     * @return int
     */
    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string|object $job
     * @param  mixed $data
     * @param  string|null $queue
     * @return mixed
     */
    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));
        $this->producer->flush(1000);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string|null $queue
     * @param  array $options
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTimeInterface|\DateInterval|int $delay
     * @param  string|object $job
     * @param  mixed $data
     * @param  string|null $queue
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    /**
     * Pop the next job off of the queue.
     * @param null $queue
     * @return \Illuminate\Contracts\Queue\Job|null|void
     * @throws \Exception
     */
    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);
        $message = $this->consumer->consume(120*1000);
        try {
            switch ($message->err){
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more message; will wait for more\n");
                    break;

                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("time out\n");
                    break;

                default:
                    throw new \Exception($message->errst(), $message->err);
            }
        } catch (Exception $e){
            var_dump($e->getMessage());
        }
    }
}
