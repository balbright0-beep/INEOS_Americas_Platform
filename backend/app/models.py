from sqlalchemy import Column, Integer, String, Text, DateTime, func
from app.database import Base


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False)  # admin, internal, dealer
    dealer_name = Column(String)  # only for dealer accounts
    created_at = Column(DateTime, server_default=func.now())


class Bulletin(Base):
    __tablename__ = "bulletins"
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    priority = Column(String, default="info")  # info, important, urgent
    audience = Column(String, default="both")  # internal, dealer, both
    created_by = Column(String)
    created_at = Column(DateTime, server_default=func.now())


class LinkCategory(Base):
    __tablename__ = "link_categories"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    sort_order = Column(Integer, default=0)


class Link(Base):
    __tablename__ = "links"
    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    description = Column(String)
    sort_order = Column(Integer, default=0)


class AppState(Base):
    __tablename__ = "app_state"
    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(Text)
