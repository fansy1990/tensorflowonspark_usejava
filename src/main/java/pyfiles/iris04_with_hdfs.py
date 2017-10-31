
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from tensorflowonspark import TFCluster

import tensorflow as tf
import argparse
# import data_dist


def map_fun(args, ctx):
  from tensorflowonspark import TFNode
  from datetime import datetime
  import numpy
  import tensorflow as tf
  import time
  import math

  worker_num = ctx.worker_num
  job_name = ctx.job_name
  task_index = ctx.task_index
  cluster_spec = ctx.cluster_spec

  # Delay PS nodes a bit, since workers seem to reserve GPUs more quickly/reliably (w/o conflict)
  if job_name == "ps":
    time.sleep((worker_num + 1) * 5)

  # Parameters
  batch_size   = args.batch_size

  # Get TF cluster and server instances
  cluster, server = TFNode.start_cluster_server(ctx, 1, args.rdma)

  def feed_dict(batch):
    # Convert from [images_labels] to two numpy arrays of the proper type
    images = []
    labels = []
    for item in batch:
      images.append(item[0: 4])
      labels.append(item[4])
    xs = numpy.array(images)
    xs = xs.astype(numpy.float32)
    ys = dense_to_one_hot(numpy.array(labels, dtype=numpy.uint), 3)
    ys = ys.astype(numpy.uint8)
    return (xs, ys)

  def dense_to_one_hot(labels_dense, num_classes):
    """Convert class labels from scalars to one-hot vectors."""
    num_labels = labels_dense.shape[0]
    index_offset = numpy.arange(num_labels) * num_classes
    labels_one_hot = numpy.zeros((num_labels, num_classes))
    tt = index_offset + labels_dense.ravel()
    tt = tt.astype(numpy.int32)
    labels_one_hot.flat[tt] = 1
    return labels_one_hot

  if job_name == "ps":
    server.join()
  elif job_name == "worker":

    # Assigns ops to the local worker by default.
    with tf.device(tf.train.replica_device_setter(
        worker_device="/job:worker/task:%d" % task_index,
        cluster=cluster)):

        # network
        x = tf.placeholder(tf.float32, [None, 4])

        # paras
        W = tf.Variable(tf.zeros([4, 3]))
        b = tf.Variable(tf.zeros([3]))

        y = tf.nn.softmax(tf.matmul(x, W) + b)
        y_ = tf.placeholder(tf.float32, [None, 3])

        # loss func
        cross_entropy = -tf.reduce_sum(y_ * tf.log(y))

        global_step = tf.Variable(0)

        train_op = tf.train.GradientDescentOptimizer(0.01).minimize(cross_entropy,global_step=global_step )

        # Test trained model
        label = tf.argmax(y_, 1, name="label") #??? does the function argmax use in the right way ?
        prediction = tf.argmax(y, 1, name="prediction")
        correct_prediction = tf.equal(prediction, label)

        accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32), name="accuracy")
        tf.summary.scalar("acc", accuracy)

        saver = tf.train.Saver()
        summary_op = tf.summary.merge_all()
        init_op = tf.global_variables_initializer()
    # Create a "supervisor", which oversees the training process and stores model state into HDFS
    logdir = TFNode.hdfs_path(ctx, args.model)
    print("tensorflow model path: {0}".format(logdir))
    summary_writer = tf.summary.FileWriter("tensorboard_%d" %(worker_num), graph=tf.get_default_graph())

    if args.mode == "train":
      sv = tf.train.Supervisor(is_chief=(task_index == 0),
                               logdir=logdir,
                               init_op=init_op,
                               summary_op=None,
                               saver=saver,
                               global_step=global_step,
                               stop_grace_secs=300,
                               save_model_secs=1)

    # The supervisor takes care of session initialization, restoring from
    # a checkpoint, and closing when done or an error occurs.
    with sv.managed_session(server.target) as sess:
      print("{0} session ready".format(datetime.now().isoformat()))

      # Loop until the supervisor shuts down or 1000000 steps have completed.
      step = 0
      tf_feed = TFNode.DataFeed(ctx.mgr, args.mode == "train")
      while not sv.should_stop() and not tf_feed.should_stop() and step < args.steps:
        # Run a training step asynchronously.
        # See `tf.train.SyncReplicasOptimizer` for additional details on how to
        # perform *synchronous* training.

        # using feed_dict
        batch_xs, batch_ys = feed_dict(tf_feed.next_batch(batch_size))
        feed = {x: batch_xs, y_: batch_ys}

        if len(batch_xs) > 0:
          if args.mode == "train":
            _, summary, step = sess.run([train_op, summary_op, global_step], feed_dict=feed)
            # print accuracy and save model checkpoint to HDFS every 100 steps
            if (step % 100 == 0):
              print("{0} step: {1} accuracy: {2}".format(datetime.now().isoformat(), step,
                                                         sess.run(accuracy,{x: batch_xs, y_: batch_ys})))

            if sv.is_chief:
              summary_writer.add_summary(summary, step)


      if sv.should_stop() or step >= args.steps:
        tf_feed.terminate()

    # Ask for all the services to stop.
    print("{0} stopping supervisor".format(datetime.now().isoformat()))
    sv.stop()




if __name__ == '__main__':
    sc = SparkContext(conf=SparkConf().setAppName("read hdfs save to hdfs "))
    executors = sc._conf.get("spark.executor.instances")
    num_executors = int(executors) if executors is not None else 1
    num_ps = 1

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="input hdfs path")
    parser.add_argument("-m", "--model", help="HDFS path to save/load model during train/inference",
                        default="mnist_model")
    parser.add_argument("-tb", "--tensorboard", help="launch tensorboard process", default=False)
    parser.add_argument("-b", "--batch_size", help="number of records per batch", type=int, default=100)
    parser.add_argument("-e", "--epochs", help="number of epochs", type=int, default=1)
    parser.add_argument("-s", "--steps", help="maximum number of steps", type=int, default=1000)
    parser.add_argument("-X", "--mode", help="train|inference", default="train")
    parser.add_argument("-c", "--rdma", help="use rdma connection", default=False)

    args = parser.parse_args()
    print("args:", args)

    # read data
    input_data = sc.textFile(args.input).map(lambda ln: [float(x) for x in ln.split(',')])

    cluster = TFCluster.run(sc, map_fun, args, num_executors,
                            num_ps, args.tensorboard,
                            TFCluster.InputMode.SPARK)
    cluster.train(input_data, 1)

    cluster.shutdown()	