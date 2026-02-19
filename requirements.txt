from flask import Flask, render_template, send_from_directory, abort
import os

app = Flask(__name__)

# ── Contraseña de acceso simple (cámbiala cuando quieras) ──
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN", "valmex2024")

@app.route("/")
def index():
    return render_template("valmex_dashboard.html")

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
