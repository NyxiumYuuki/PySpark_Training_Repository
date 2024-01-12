from pyspark.sql import DataFrame


def pyspark_transforms(*args, **kwargs):

    def wrapper(*args, **kwargs):
        print(args)
        print(kwargs)

    return wrapper


def Input(url: str) -> None:

    return None


def Output(url: str):
    pass