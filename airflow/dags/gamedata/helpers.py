from io import BytesIO
from minio import Minio, S3Error
import pandas as pd


client = Minio(
    "lorenz:9000",
    access_key="airflowuser",
    secret_key="6Q6Gat93BArEg3oAd4NLhFjqhynM",
    secure=False
)


def get_s3_file(path):
    obj = client.get_object(
        "datalake",
        path,
    )
    df = pd.read_csv(obj)
    return df


def put_s3_file(df, path):
    csv = df.to_csv().encode('utf-8')
    client.put_object(
        "datalake",
        path,
        data=BytesIO(csv),
        length=len(csv),
        content_type='application/csv'
    )


def s3_get_nth_prev_object(path, n):
    """For versioned objects this gets the n'th previous version

    n=0 means the current version, n=1 is the previous version, etc...
    """
    obj_list = client.list_objects('datalake', include_version=True, recursive=True)
    obj_list = filter(lambda o: (o.last_modified != None) & (o.object_name == path), obj_list)
    # Sort by modified date, oldest first
    obj_list = sorted(obj_list, key=lambda o: o.last_modified, reverse=True)
    return obj_list[n]


def s3_path_exists(path):
    try:
        client.stat_object("datalake", path)
    except S3Error:
        return False
    else:
        return True

def generate_genre_map(df):
    """Create a base mapping to a standardized genre name from the mess that Wikipedia has.

    This function takes a DataFrame (df) from the Wikipedia job.
    """
    # Explode tuples
    genres_full = df['Genre'].explode('Genre')
    # Split any remaining comma seperated lists
    genres_full = genres_full.str.split(',', expand=True)
    genres_full = genres_full[0].to_frame(name="Genre").merge(genres_full[1].to_frame(name="Genre"), how='outer', on='Genre')
    genres_full = genres_full['Genre']
    genres_full = genres_full[genres_full.notnull()]

    # Create the mapping dataframe
    genre_map = pd.DataFrame({
        'from': genres_full,
        'to': genres_full
    })

    # Capitalize words
    genre_map['to'] = genre_map['to'].map(lambda a: a.title())

    # Reduce to unique entries
    genre_map = genre_map.drop_duplicates(subset=['from'])

    genre_map = genre_map.sort_values('from')

    return genre_map
