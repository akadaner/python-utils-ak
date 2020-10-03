""" Email functionality. """
import pyzmail
import json
from smtplib import SMTP_SSL
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import email.encoders as Encoders


def send_gmail(username, password, to_, subject, text, html=False):
    # compose mail
    sender = (u'I3K', username)
    # recipients = [(u'Him', to_), username]
    if type(to_) == str:
        to_ = [to_]
    if type(to_) != list:
        raise Exception('wrong to_ format')

    recipients = [(u'Good Guy', t) for t in to_]
    subject = subject
    text_content = text
    # prefered_encoding = 'iso-8859-1'
    # text_encoding = 'iso-8859-1'
    prefered_encoding = 'utf-8'
    text_encoding = 'utf-8'

    if html:
        payload, mail_from, rcpt_to, msg_id = pyzmail.compose_mail(sender, recipients, subject, prefered_encoding, None, html=(text, text_encoding), attachments=None)
    else:
        payload, mail_from, rcpt_to, msg_id = pyzmail.compose_mail(sender, recipients, subject, prefered_encoding, (text_content, text_encoding), html=None, attachments=None)

    # send mail
    smtp_host = 'smtp.gmail.com'
    smtp_port = 587
    smtp_mode = 'tls'
    smtp_login = username
    smtp_password = password

    # sends to our mail too
    # pyzmail.send_mail(payload, mail_from, rcpt_to, smtp_host, smtp_port=smtp_port, smtp_mode=smtp_mode, smtp_login=smtp_login, smtp_password=smtp_password)

    return pyzmail.send_mail(payload, None, rcpt_to, smtp_host, smtp_port=smtp_port, smtp_mode=smtp_mode, smtp_login=smtp_login, smtp_password=smtp_password)


def send_yandex(username, password, to_, subject, text, html=False, headers=[], sender_name=u'Good boy'):
    # compose mail
    sender = (sender_name, username)
    # recipients = [(u'Him', to_), username]
    if type(to_) == str:
        to_ = [to_]
    if type(to_) != list:
        raise Exception('wrong to_ format')

    recipients = [(t, t) for t in to_]
    subject = subject
    text_content = text
    # prefered_encoding = 'iso-8859-1'
    # text_encoding = 'iso-8859-1'
    prefered_encoding = 'utf-8'
    text_encoding = 'utf-8'

    if html:
        payload, mail_from, rcpt_to, msg_id = pyzmail.compose_mail(sender, recipients, subject, prefered_encoding, None, html=(text, text_encoding), attachments=None, headers=headers)
    else:
        payload, mail_from, rcpt_to, msg_id = pyzmail.compose_mail(sender, recipients, subject, prefered_encoding, (text_content, text_encoding), html=None, attachments=None, headers=headers)

    # send mail
    smtp_host = 'smtp.yandex.ru'
    smtp_port = 0
    smtp_mode = 'ssl'
    smtp_login = username
    smtp_password = password

    return pyzmail.send_mail(payload, username, rcpt_to, smtp_host, smtp_port=smtp_port, smtp_mode=smtp_mode, smtp_login=smtp_login, smtp_password=smtp_password)


if __name__ == '__main__':
    msg = dict(a=u'аладушек', b=2)
    print(send_gmail('email_from', 'password_here', ['emails_to'], 'subject', json.dumps(msg, indent=2, ensure_ascii=False).encode('utf-8')))
