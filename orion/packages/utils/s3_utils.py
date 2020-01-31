import pickle
import boto3


def store_on_s3(data, bucket, prefix):
    """Writes out data to s3 as pickle, so it can be picked up by a task.

    Args:
        data (:obj:`list` of :obj:`str`): A batch of records.
        bucket (str): Name of the s3 bucket.
        prefix (str): Identifier for the batched object.

    Returns:
        (str): name of the file in the s3 bucket (key).
    
    """
    # Pickle data
    data = pickle.dumps(data)

    # s3 setup
    s3 = boto3.resource("s3")
    # timestamp = str(time.time()).replace('.', '')
    filename = f"{prefix}.pickle"
    obj = s3.Object(bucket, filename)
    obj.put(Body=data)

    return filename


def load_from_s3(bucket, prefix):
    """Loads a pickled file from s3.

    Args:
       bucket (str): Name of the s3 bucket.
       prefix (str): Name of the pickled file.

    """
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket, f"{prefix}.pickle")
    return pickle.loads(obj.get()["Body"].read())


def s3_bucket_obj(bucket):
    """Get all objects of an S3 bucket.

    Args:
       bucket (str): Name of the s3 bucket.
    
    Returns:
        (`boto3.resources.collection.s3.Bucket.objectsCollection`)
    
    """
    s3 = boto3.resource("s3")
    return s3.Bucket(bucket).objects.all()
