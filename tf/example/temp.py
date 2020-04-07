def train_nn_regression_model(
    my_optimizer,
    steps,
    periods,
    batch_size,
    hidden_units,
    training_examples,
    training_targets,
    validation_examples,
    validation_targets,
    model_dir,
    warm_start_from=None):
    """Trains a neural network regression model.  
        In addition to training, this function also prints training progress information,
        as well as a plot of the training and validation loss over time.

    Args:
        my_optimizer: An instance of `tf.train.Optimizer`, the optimizer to use.
        steps: A non-zero `int`, the total number of training steps. A training step
            consists of a forward and backward pass using a single batch.
        batch_size: A non-zero `int`, the batch size.
        hidden_units: A `list` of int values, specifying the number of neurons in each layer.
        training_examples: A `DataFrame` containing one or more columns from
            the training set to use as input features for training.
        training_targets: A `DataFrame` containing exactly one column from
            the training set to use as target for training.
        validation_examples: A `DataFrame` containing one or more columns from
            the validation set to use as input features for validation.
        validation_targets: A `DataFrame` containing exactly one column from
            the validation set to use as target for validation.
        
    Returns:
        A tuple `(estimator, training_losses, validation_losses)`:
            estimator: the trained `DNNRegressor` object.
            training_losses: a `list` containing the training loss values taken during training.
            validation_losses: a `list` containing the validation loss values taken during training.
    """
    steps_per_period = steps / periods
    # Create a DNNRegressor object.
    my_optimizer = tf.contrib.estimator.clip_gradients_by_norm(my_optimizer, 5.0)
    dnn_regressor = tf.estimator.DNNRegressor(
        feature_columns=construct_feature_columns(training_examples),
        hidden_units=hidden_units,
        optimizer=my_optimizer,
        model_dir=model_dir,
        warm_start_from=warm_start_from
    )
    # Create input functions.
    training_input_fn = lambda: my_input_fn(training_examples, 
                                          training_targets["Size"], 
                                          batch_size=batch_size)
    predict_training_input_fn = lambda: my_input_fn(training_examples, 
                                                  training_targets["Size"], 
                                                  num_epochs=1, 
                                                  shuffle=False)
    predict_validation_input_fn = lambda: my_input_fn(validation_examples, 
                                                    validation_targets["Size"], 
                                                    num_epochs=1, 
                                                    shuffle=False)

    # Train the model, but do so inside a loop so that we can periodically assess
    # loss metrics.
    print("Training model...")
    print("RMSE (on training data):")
    training_rmse = []
    validation_rmse = []
    
    # Whenever you need to record the loss, feed the mean loss to this placeholder
    training_rmse_ph = tf.placeholder(tf.float32,shape=None,name='training_rmse')
    # Create a scalar summary object for the loss so it can be displayed
    training_rmse_summary = tf.summary.scalar('training_rmse', training_rmse_ph)
    
    validation_rmse_ph = tf.placeholder(tf.float32,shape=None, name='validation_rmse')
    validation_rmse_summary = tf.summary.scalar('validation_rmse', validation_rmse_ph)
    # Merge all summaries together
    performance_summaries = tf.summary.merge([training_rmse_summary,validation_rmse_summary])
    with tf.name_scope('error_performance'):
        for period in range (0, periods):
            # Train the model, starting from the prior state.
            dnn_regressor.train(
                input_fn=training_input_fn,
                steps=steps_per_period
            )
            # Take a break and compute predictions.
            training_predictions = dnn_regressor.predict(input_fn=predict_training_input_fn)
            training_predictions = np.array([item['predictions'][0] for item in training_predictions])

            validation_predictions = dnn_regressor.predict(input_fn=predict_validation_input_fn)
            validation_predictions = np.array([item['predictions'][0] for item in validation_predictions])

            # Compute training and validation loss.
            training_root_mean_squared_error = math.sqrt(
                metrics.mean_squared_error(training_predictions, training_targets))
            validation_root_mean_squared_error = math.sqrt(
                metrics.mean_squared_error(validation_predictions, validation_targets))
            # Occasionally print the current loss.
            print("  period %02d : %0.2f" % (period, training_root_mean_squared_error))
            # Add the loss metrics from this period to our list.
            training_rmse.append(training_root_mean_squared_error)
            validation_rmse.append(validation_root_mean_squared_error)

            training_rmse_ph = training_root_mean_squared_error
            validation_rmse_ph = validation_root_mean_squared_error
    print("Model training finished.")
    # Output a graph of loss metrics over periods.
    plt.ylabel("RMSE")
    plt.xlabel("Periods")
    plt.title("Root Mean Squared Error vs. Periods")
    plt.tight_layout()
    plt.plot(training_rmse, label="training")
    plt.plot(validation_rmse, label="validation")
    plt.legend()
    plt.ylim(bottom=0)  # turn off y axis autoscaling
    plt.ylim(top=50)
    print("Final RMSE (on training data):   %0.2f" % training_root_mean_squared_error)
    print("Final RMSE (on validation data): %0.2f" % validation_root_mean_squared_error)
    return dnn_regressor, training_rmse, validation_rmse