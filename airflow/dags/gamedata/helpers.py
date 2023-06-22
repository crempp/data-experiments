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
