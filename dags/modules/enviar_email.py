import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def enviar_email(**context):
    from_address = context["var"]["value"].get("email")
    password = context["var"]["value"].get("email_password")
    to_address = context["var"]["value"].get("to_address")

    # Traemos la información que utilizaremos en el correo.
    datos = context['ti'].xcom_pull(task_ids='obtener_datos', key='datos')
    print(datos)

    message = MIMEMultipart()
    message['From'] = from_address
    message['To'] = to_address

    # Cremos una condicion para elegir el asunto y el contenido del mail.
    if datos[0][1] >= 23:
        message['Subject'] = f'Aviso del clima: {datos[0][4]}!'
        html_content = f"""
            <html>
                <body>
                    <h2>Hoy sera un día caluroso en el departamento de {datos[0][0]}!</h2>
                    <h3>Con una sensación térmica de {datos[0][1]}°C.</h3>
                    <ul>
                        <li>Clima: {datos[0][4]}.</li>
                        <li>Descripción: {datos[0][5]}.</li>
                        <li>Temperatura máxima: {datos[0][3]}°C.</li>
                        <li>Temperatura mínima: {datos[0][2]}°C.</li>
                        <li>Recomendación: <strong>Llevar ropa fresca.</strong></li>
                    </ul>
                    <h3>Que tengas un buen día!</h3>
                    <br>
                    <p>Actualizado: {datos[0][6]}.</p>
                </body>
            </html>
        """
    elif datos[0][1] >= 18:
        message['Subject'] = f'Aviso del clima: {datos[0][4]}!'
        html_content = f"""
            <html>
                <body>
                    <h2>Hoy sera un día cálido en el departamento de {datos[0][0]}!</h2>
                    <h3>Con una sensación térmica de {datos[0][1]}°C.</h3>
                    <ul>
                        <li>Clima: {datos[0][4]}.</li>
                        <li>Descripción: {datos[0][5]}.</li>
                        <li>Temperatura máxima: {datos[0][3]}°C.</li>
                        <li>Temperatura mínima: {datos[0][2]}°C.</li>
                        <li>Recomendación: <strong>Llevar un abrigo ligero.</strong></li>
                    </ul>
                    <h3>Que tengas un buen día!</h3>
                    <br>
                    <p>Actualizado: {datos[0][6]}.</p>
                </body>
            </html>
        """
    else:
        message['Subject'] = f'Aviso del clima: {datos[0][4]}!'
        html_content = f"""
            <html>
                <body>
                    <h2>Hoy sera un día frío en el departamento de {datos[0][0]}!</h2>
                    <h3>Con una sensación térmica de {datos[0][1]}°C.</h3>
                    <ul>
                        <li>Clima: {datos[0][4]}.</li>
                        <li>Descripción: {datos[0][5]}.</li>
                        <li>Temperatura máxima: {datos[0][3]}°C.</li>
                        <li>Temperatura mínima: {datos[0][2]}°C.</li>
                        <li>Recomendación: <strong>Llevar abrigo.</strong></li>
                    </ul>
                    <h3>Que tengas un buen día!</h3>
                    <br>
                    <p>Actualizado: {datos[0][6]}.</p>
                </body>
            </html>
        """
    
    message.attach(MIMEText(html_content, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_address, password)
        text = message.as_string()
        server.sendmail(from_address, to_address, text)
        server.quit()
        print("Correo enviado correctamente!!")
    except Exception as e:
        print(f"Hubo un problema: {str(e)}")
    