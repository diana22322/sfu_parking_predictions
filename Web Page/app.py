from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

LOTS = {
"burnaby": {
"West Parkade": {"total": 300, "occupied": 204},
"South Parkade": {"total": 150, "occupied": 150},
"East Parking": {"total": 320, "occupied": 5},
"North Parking": {"total": 100, "occupied": 56},
"South Parking": {"total": 67, "occupied": 20},
"Residence West Lot": {"total": 298, "occupied": 230}
},
"surrey": {
"Surrey South": {"total": 234, "occupied": 204},
"Surrey City Centre Main Lot": {"total": 150, "occupied": 30}
}
}

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/burnaby")
def burnaby():
    return render_template(
        "campus.html",
        campus_key="burnaby",
        campus_name="Burnaby Campus",
        accent="#A6192E")

@app.route("/surrey")
def surrey():
    return render_template(
        "campus.html",
        campus_key="surrey",
        campus_name="Surrey Campus",
        accent="#54585A")

@app.route("/log_event")
def log_event():
    return render_template("log_event.html")

@app.route("/api/lots/<campus>")
def api_lots(campus):
    campus = campus.lower()
    data = LOTS.get(campus)
    if not data:
        return jsonify({"error": "unknown campus"}), 404
    lots = [{"name": k, **v} for k, v in sorted(data.items())]
    return jsonify({"campus": campus, "lots": lots})

@app.route("/api/log", methods=["POST"])
def api_log():
    if not request.is_json:
        return jsonify({"ok": False, "error": "expected application/json"}), 400
    payload = request.get_json(silent=True) or {}
    campus = (payload.get("campus") or "").lower()
    lot    = payload.get("lot")
    typ    = payload.get("type")

    if campus not in LOTS or lot not in LOTS[campus] or typ not in {"entry", "exit"}:
        return jsonify({"ok": False, "error": "invalid campus/lot/type"}), 400

    return jsonify({"ok": True, "received": payload}), 201

if __name__ == "__main__":
    app.run(debug=True)