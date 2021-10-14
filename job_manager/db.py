
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from . import settings

engine = sa.create_engine(settings.DATABASE_CONNECTION_STRING)
Session = sessionmaker(bind=engine)
