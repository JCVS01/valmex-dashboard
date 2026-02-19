from flask import Flask, send_file
import os

app = Flask(__name__)

@app.route("/")
def index():
    return send_file("valmex_dashboard.html")

@app.route("/presentacion.pdf")
def pdf():
    return send_file("PRESENTACIO_N_COMERCIAL.pdf")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
