from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Table

from datetime import datetime

Base = declarative_base()


class IdMixin:
    id = Column(Integer, primary_key=True, autoincrement=True)


class UrlMixin:
    url = Column(String(length=256), nullable=False, unique=True)


class NameMixin:
    name = Column(String, nullable=False)


tag_post = Table(
    "tag_post",
    Base.metadata,
    Column("post.id", Integer, ForeignKey("post.id")),
    Column("tag_id", Integer, ForeignKey("tag.id")),
)


class Post(Base, UrlMixin):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    create_at = Column(DateTime, nullable=False)

    author_id = Column(Integer, ForeignKey("author.id"))
    author = relationship("Author")

    tags = relationship("Tag", secondary=tag_post)
    comments = relationship("Comment")


class Author(Base, IdMixin, UrlMixin, NameMixin):
    __tablename__ = "author"
    posts = relationship("Post")


class Tag(Base, IdMixin, UrlMixin, NameMixin):
    __tablename__ = "tag"
    posts = relationship("Post", secondary=tag_post)


class Comment(Base):
    __tablename__ = "comment"

    id = Column(Integer, primary_key=True, autoincrement=False)
    parent_id = Column(Integer, ForeignKey("comment.id"), nullable=True)
    likes_count = Column(Integer)
    body = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=True)
    time_now = Column(DateTime, nullable=True)
    hidden = Column(Boolean)
    deep = Column(Integer)

    author_id = Column(Integer, ForeignKey("author.id"))
    author = relationship("Author")

    post_id = Column(Integer, ForeignKey("post.id"))
    post = relationship("Post")

    time_now = Column(DateTime)

    def __init__(self, **kwargs):
        self.id = kwargs["id"]
        self.parent_id = kwargs["parent_id"]
        self.likes_count = kwargs["likes_count"]
        self.body = kwargs["body"]
        self.create_at = datetime.fromisoformat(kwargs["created_at"])
        self.hidden = kwargs["hidden"]
        self.deep = kwargs["deep"]
        self.time_now = datetime.fromisoformat(kwargs["time_now"])
        self.author = kwargs["author"]
