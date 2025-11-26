def format_table(columns, rows):
    if not rows:
        return {
            'columns': columns,
            'rows': [],
            'message': 'Nenhum resultado encontrado.'
        }

    return {
        'columns': columns,
        'rows': [list(r) for r in rows]
    }