from flask import Flask, request, jsonify, app

app = Flask(__name__)


@app.route('/datos', methods=["POST"])
def datos():
    print(request.is_json)
    content = request.get_json()
    print(content)
    return 'JSON posted'




def iniciar():
    app.run(host='0.0.0.0', port=8000)
