
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker 

engine = sa.create_engine("sqlite:///db.sqlite",connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine)

