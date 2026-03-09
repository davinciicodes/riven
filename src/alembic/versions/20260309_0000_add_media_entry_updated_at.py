"""CDN health uses FilesystemEntry.updated_at (no schema change needed)

Revision ID: add_media_entry_updated_at
Revises: b1345f835923
Create Date: 2026-03-09 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

revision: str = "add_media_entry_updated_at"
down_revision: Union[str, None] = "b1345f835923"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
