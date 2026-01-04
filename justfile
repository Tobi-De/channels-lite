set dotenv-load := true

# List all available commands
_default:
    @just --list --unsorted

@runserver:
    cd demo && uv run python manage.py runserver

@dj *ARGS:
    cd demo && uv run python manage.py {{ ARGS }}

    
