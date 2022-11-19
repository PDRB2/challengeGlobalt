# challengeGlobalt
Create README.md
Desafio consistente en crear un modelo de datos y exponerlo mediante una api, con las complejidades que todo manejo de volumenes pequenios puede conllevar, con una base portable y validaciones de datos.
Se presenta el formato de los metodos existentes y el formato correcto de los json en caso que se nesesite.
Ademas cuenta con una pequeña validacion de seguridad.

metodo post.
http://192.168.0.8:8000/datos
json esperado de ejemplo
{ "tables":[
     {"table": "hired_employees",
    "record_info": [{"id":3001,"name":"Carlos","datetime":"2021-11-07T02:48:42Z","department_id":5,"job_id":100}] } ]}
con las posibilidades de enviar mas de una tabla.
Presenta las reglas que no se pueden enviar mas de mil registros por llamada, esto quedara registrado pero no se insertara.
otra regla es si posee errores en los metadatos esperados no se ingresara.
Adicionalmente si existen los registros internamente, no se cargaran y se informara por log.

otro metodos que se crearon son para el pasaje de reportes correspondiente a dos metricas
http://127.0.0.1:8000/metricas1
y
http://127.0.0.1:8000/metricas2

ambos devolveran un json , si la autorizacion es correspondiente.
Nota de desarrollo, el metodo metricas 2 nesesita aproximadamente que se inserten aproximadamente 2 mil registros extras para que devuelva una respuesta que corresponda con el desafio de la metrica.

Notas finales:
Por motivos de desarrollo y ahorro de tiempo , existe una modificacion que se puede hacer para que cada vez que se corra la api vacie y recarge la tabla