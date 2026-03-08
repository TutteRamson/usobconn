"""Feedback feature routes — Flask Blueprint.

Register this blueprint in app.py to enable the Feedback tab.
All endpoints use SQLAlchemy parameterised queries (no raw SQL).
"""

import functools
import os
import secrets

from flask import Blueprint, jsonify, redirect, request, session, url_for

from models import db
from feedback_models import FeedbackItem, FeedbackResponse

feedback_bp = Blueprint("feedback", __name__)

# Re-use the same auth pattern as the main app
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")


def _login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if APP_PASSWORD and not session.get("authenticated"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


# ---------- List all feedback items ----------
@feedback_bp.route("/api/feedback")
@_login_required
def list_feedback():
    items = FeedbackItem.query.order_by(FeedbackItem.created_at.desc()).all()
    return jsonify([item.to_dict(include_responses=True) for item in items])


# ---------- Create a new feedback item ----------
@feedback_bp.route("/api/feedback", methods=["POST"])
@_login_required
def create_feedback():
    data = request.get_json(silent=True) or {}
    fb_type = (data.get("type") or "").strip()
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()

    if fb_type not in ("change_request", "bug"):
        return jsonify({"error": "type must be 'change_request' or 'bug'"}), 400
    if not title:
        return jsonify({"error": "title is required"}), 400
    if not description:
        return jsonify({"error": "description is required"}), 400

    user_agent = request.headers.get("User-Agent", "")
    item = FeedbackItem(type=fb_type, title=title, description=description, user_agent=user_agent)
    db.session.add(item)
    db.session.commit()
    return jsonify(item.to_dict(include_responses=True)), 201


# ---------- Respond to a feedback item (address / re-open) ----------
@feedback_bp.route("/api/feedback/<int:item_id>/respond", methods=["POST"])
@_login_required
def respond_to_feedback(item_id):
    item = db.session.get(FeedbackItem, item_id)
    if not item:
        return jsonify({"error": "Feedback item not found"}), 404

    data = request.get_json(silent=True) or {}
    action = (data.get("action") or "").strip()
    response_text = (data.get("response_text") or "").strip()

    if action not in ("addressed", "re-opened"):
        return jsonify({"error": "action must be 'addressed' or 're-opened'"}), 400
    if not response_text:
        return jsonify({"error": "response_text is required"}), 400

    # Validate transitions
    if action == "addressed" and item.status == "addressed":
        return jsonify({"error": "Item is already addressed"}), 400
    if action == "re-opened" and item.status != "addressed":
        return jsonify({"error": "Only addressed items can be re-opened"}), 400

    resp = FeedbackResponse(
        feedback_item_id=item.id,
        action=action,
        response_text=response_text,
    )
    item.status = action
    db.session.add(resp)
    db.session.commit()
    return jsonify(item.to_dict(include_responses=True))
