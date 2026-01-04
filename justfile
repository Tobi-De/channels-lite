set dotenv-load := true

# List all available commands
_default:
    @just --list --unsorted

@runserver:
    cd demo && uv run python manage.py runserver

@dj *ARGS:
    cd demo && uv run python manage.py {{ ARGS }}

# Bump version, commit, tag, and push (usage: just release patch|minor|major|X.Y.Z)
release VERSION:
    #!/usr/bin/env bash
    set -euo pipefail

    # Get current version
    CURRENT_VERSION=$(grep '^version = ' pyproject.toml | cut -d'"' -f2)
    echo "Current version: $CURRENT_VERSION"

    # Calculate new version
    if [[ "{{ VERSION }}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        NEW_VERSION="{{ VERSION }}"
    else
        IFS='.' read -r major minor patch <<< "$CURRENT_VERSION"
        case "{{ VERSION }}" in
            major)
                NEW_VERSION="$((major + 1)).0.0"
                ;;
            minor)
                NEW_VERSION="$major.$((minor + 1)).0"
                ;;
            patch)
                NEW_VERSION="$major.$minor.$((patch + 1))"
                ;;
            *)
                echo "Error: VERSION must be 'major', 'minor', 'patch', or a version number (X.Y.Z)"
                exit 1
                ;;
        esac
    fi

    echo "New version: $NEW_VERSION"

    # Update version in pyproject.toml
    sed -i "s/^version = \".*\"/version = \"$NEW_VERSION\"/" pyproject.toml

    # Commit and tag
    git add pyproject.toml
    git commit -m "Bump version to $NEW_VERSION"
    git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"

    # Push commit and tags
    git push
    git push --tags

    echo "✓ Released version $NEW_VERSION"

