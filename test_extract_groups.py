# %%
import polars  as pl

# %%

test_df = pl.read_csv('data/certificates.csv')

# %%
test_df.glimpse()

# %%
test_df.columns

# %%

# optimise the processing of construction age bands with polars expressions

def extract_construction_age_band(df):
    out_df = (df
     .with_columns([
         pl.col('CONSTRUCTION_AGE_BAND')
         .str.extract_groups(r'(\b\d{4}\b)-(\b\d{4}\b)')
         .alias('years_struct')
         .cast(pl.Int32).struct
         .rename_fields(['start', 'end']),

         pl.col('CONSTRUCTION_AGE_BAND')
         .str.extract_groups(r'(\d{4}\b)')
         .alias('year_struct')
         .cast(pl.Int32).struct
         .rename_fields(['only_year'])
         ])

        .with_columns(pl.col('years_struct'))
        .unnest('years_struct')
         
        .with_columns(pl.col('year_struct')).unnest('year_struct')

        .with_columns(
            pl.when(pl.col('start').is_null())
            .then(pl.col('only_year'))
            .otherwise(pl.mean_horizontal(['start', 'end'])).round().cast(pl.Int32)
            .alias('mean_year')
            )
        .with_columns(
            pl.when(pl.col('only_year') == (pl.col('start')))
            .then(None)
            .otherwise(pl.col('only_year')).alias('only_year')
        )
        .with_columns(
            pl.when(pl.col('only_year') == 1900)
            .then(pl.lit('Before 1900'))
            .when(pl.col('only_year') > 1900)
            .then(pl.concat_str([
                pl.lit('After '),
                pl.col('only_year')]))
            .otherwise(pl.concat_str([
                pl.lit('Between '),
                pl.col('start'),
                pl.lit(' and '),
                pl.col('end')]))
                .alias('construction_age_band_clean')
        )
        
    )
    return out_df

# %%
out_df = test_df.pipe(extract_construction_age_band)


# %%

out_df.select(['only_year', 'start', 'end', 'mean_year', 'construction_age_band_clean']).glimpse()

# %%S
out_df.glimpse()

#out_df.select(pl.col([ 'years_struct', 'year'])).head(10)
# %%

def clean_tenure(df):
    tenure_df = (
        df
        .with_columns(
            pl.when(pl.col('TENURE').str.contains('wner-occupied'))
            .then(pl.lit('Owner occupied'))
            .when(pl.col('TENURE').str.contains('social'))
            .then(pl.lit('Social rented'))
            .when(pl.col('TENURE').str.contains('private'))
            .then(pl.lit('Private rented'))
            .otherwise(pl.lit('Unknown'))
            .alias('tenure_clean')
            )
        
    )
    return tenure_df
# %%

tenure_df = out_df.pipe(clean_tenure)

tenure_df.head(10)
# %%



# %%



# %%