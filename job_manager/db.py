
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from . import settings

engine = sa.create_engine(settings.DATABASE_CONNECTION_STRING, max_overflow = -1, pool_timeout = 600.0)
Session = sessionmaker(bind=engine)
