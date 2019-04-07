from source import open_csv, get_schema, verify_integrity, change_columns, adjust_total,drop_columns

def app():
    same_values = ['speed','attack','sp_def','sp_atk','defense','hp']
    file_names = ['pokemon', 'pokemon_det']
    pokemon = []
    for df in file_names:
        pokemon.append(open_csv(df))
    
    df_ok, df_nok = verify_integrity(pokemon[1], pokemon[0])
    for value in same_values:
        df_ok = change_columns(df_ok, value)
    
    df = adjust_total(df_ok)
    df_ok = drop_columns(df_ok)
    df_ok = df_ok.withColumnRenamed('is_legendary', 'legendary')
    df.write.save(path='/home/mateus/Desktop/pokemon/csv', format='csv', mode='overwrite', sep=';', header=True)
    
app()

