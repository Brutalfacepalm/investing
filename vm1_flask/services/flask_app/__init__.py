from app import Flask, render_template, send_from_directory

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)