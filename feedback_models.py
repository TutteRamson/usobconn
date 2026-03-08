"""Database models for the Feedback feature (Change Requests & Bug Reports)."""

from datetime import datetime, timezone
from models import db


class FeedbackItem(db.Model):
    """A change request or bug report submitted by a user."""

    __tablename__ = "feedback_items"

    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(20), nullable=False)  # change_request | bug
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=False)
    user_agent = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(20), nullable=False, default="new")  # new | addressed | re-opened
    created_at = db.Column(
        db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at = db.Column(
        db.DateTime, nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    responses = db.relationship(
        "FeedbackResponse", backref="feedback_item", lazy=True,
        cascade="all, delete-orphan", order_by="FeedbackResponse.created_at",
    )

    def to_dict(self, include_responses=False):
        d = {
            "id": self.id,
            "type": self.type,
            "title": self.title,
            "description": self.description,
            "user_agent": self.user_agent,
            "status": self.status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_responses:
            d["responses"] = [r.to_dict() for r in self.responses]
        return d


class FeedbackResponse(db.Model):
    """A response/action on a feedback item (address or re-open)."""

    __tablename__ = "feedback_responses"

    id = db.Column(db.Integer, primary_key=True)
    feedback_item_id = db.Column(
        db.Integer, db.ForeignKey("feedback_items.id"), nullable=False
    )
    action = db.Column(db.String(20), nullable=False)  # addressed | re-opened
    response_text = db.Column(db.Text, nullable=False)
    created_at = db.Column(
        db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self):
        return {
            "id": self.id,
            "feedback_item_id": self.feedback_item_id,
            "action": self.action,
            "response_text": self.response_text,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
