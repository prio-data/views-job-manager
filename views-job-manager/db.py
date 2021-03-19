
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker 
from tasks import Base

engine = sa.create_engine("sqlite:///db.sqlite")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

