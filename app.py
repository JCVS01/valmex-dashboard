from flask import Flask, send_file
import os

app = Flask(__name__)

@app.route("/")
def index():
    return send_file("valmex_dashboard.html")

@app.route("/PC.pdf")
def pdf():
    return send_file("PC.pdf")

@app.route("/VALMEX.png")
def logo1():
    return send_file("VALMEX.png")

@app.route("/VALMEX2.png")
def logo2():
    return send_file("VALMEX2.png")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
