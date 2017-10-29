from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from tensorflowonspark import TFCluster,TFNode
#from com.yahoo.ml.tf import TFCluster, TFNode
from datetime import datetime
import numpy as np
import sys
# from sklearn import metrics
# from sklearn import model_selection

import tensorflow as tf

X_FEATURE = 'x'  # Name of the input feature.

train_percent = 0.8


def load_data(data_file_name):
    data = np.loadtxt(open(data_file_name), delimiter=",", skiprows=0)
    return data


def data_selection(iris, train_per):
    data, target = np.hsplit(iris[np.random.permutation(iris.shape[0])], np.array([-1]))

    row_split_index = int(data.shape[0] * train_per)

    x_train, x_test = (data[1:row_split_index], data[row_split_index:])
    y_train, y_test = (target[1:row_split_index], target[row_split_index:])
    return x_train, x_test, y_train.astype(int), y_test.astype(int)


def map_run(argv, ctx):
    # Load dataset.
    data_file = 'iris01.csv'
    iris = load_data(data_file)
    # x_train, x_test, y_train, y_test = model_selection.train_test_split(
    #     iris.data, iris.target, test_size=0.2, random_state=42)

    x_train, x_test, y_train, y_test = data_selection(iris,train_percent)

    # print(x_test)
    # print(y_test)

    #
    # # Build 3 layer DNN with 10, 20, 10 units respectively.
    feature_columns = [
        tf.feature_column.numeric_column(
            X_FEATURE, shape=np.array(x_train).shape[1:])]
    classifier = tf.estimator.DNNClassifier(
        feature_columns=feature_columns, hidden_units=[10, 20, 10], n_classes=3)
    #
    # # Train.
    train_input_fn = tf.estimator.inputs.numpy_input_fn(
        x={X_FEATURE: x_train}, y=y_train, num_epochs=None, shuffle=True)
    classifier.train(input_fn=train_input_fn, steps=200)
    #
    # # Predict.
    test_input_fn = tf.estimator.inputs.numpy_input_fn(
        x={X_FEATURE: x_test}, y=y_test, num_epochs=1, shuffle=False)
    predictions = classifier.predict(input_fn=test_input_fn)
    y_predicted = np.array(list(p['class_ids'] for p in predictions))
    y_predicted = y_predicted.reshape(np.array(y_test).shape)
    # #
    # # # Score with sklearn.
    # score = metrics.accuracy_score(y_test, y_predicted)
    # print('Accuracy (sklearn): {0:f}'.format(score))
    print(np.concatenate(( y_predicted, y_test), axis= 1))
    # Score with tensorflow.
    scores = classifier.evaluate(input_fn=test_input_fn)
    print('Accuracy (tensorflow): {0:f}'.format(scores['accuracy']))

    print(classifier.params)


if __name__ == '__main__':
    import tensorflow as tf
    import sys
    sc = SparkContext(conf=SparkConf().setAppName("your_app_name"))
    executors = sc._conf.get("spark.executor.instances")
    num_executors = int(executors) if executors is not None else 1
    num_ps = 1
    tensorboard = False

    cluster = TFCluster.run(sc, map_run, sys.argv, num_executors, num_ps, tensorboard, TFCluster.InputMode.TENSORFLOW)
    cluster.shutdown()
