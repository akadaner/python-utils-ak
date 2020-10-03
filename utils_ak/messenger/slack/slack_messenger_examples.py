import time
from utils_ak.messenger.slack.slack_messenger import SlackMessenger, SimpleMessage, SlackMessage, Attachment

token = 'xoxb-316677261669-rsCabyKYTxii2AlbNgE6T9Yq'
messenger = SlackMessenger(token)


def send_message_example():
    # send with SlackMessage
    message = SimpleMessage(text='Message send test')
    message_dic = message.to_json()
    messenger.upd_message(message, '#general')
    time.sleep(1)
    # send with dict
    messenger.upd_message(message_dic, '#general')
    time.sleep(1)
    # send with str (text)
    messenger.upd_message("Message send test", '#general')


def send_and_update_example():
    messenger.upd_message("Message update test before the update", '#general', "message_unique_key")
    time.sleep(5)
    messenger.upd_message("Message update test after the update", '#general', 'message_unique_key')


def send_attachment_message():
    message = SimpleMessage(text='Message text', ts=time.time() - 300)
    message.add_field("Title 1", "Value 1")
    message.add_field("Title 2", "Value 2")
    message.add_field("Title 3", "Value 3")
    print(message.to_json())
    messenger.upd_message(message, '#general')


def send_general_slack_message_with_attachments():
    message = SlackMessage(text="This is some text")
    attachment1 = Attachment(color='#46B29D', attachment_text='Attachent1')
    attachment1.add_field('Field title', 'Field value')
    attachment2 = Attachment(footer='footer here')
    message.attachments += [attachment1, attachment2]
    messenger.upd_message(message, '#random')


if __name__ == '__main__':
    #     send_message_example()
    #     send_and_update_example()
    #
    #     """ Cache after the run
    #     {
    #  "message_timestamps": {
    #   "2018-02-16 15:31:50.736173-#general": "1518784311.000093",
    #   "2018-02-16 15:31:52.641501-#general": "1518784312.000195",
    #   "2018-02-16 15:31:53.913882-#general": "1518784313.000284",
    #   "2018-02-16 15:32:04.405777-#general": "1518784324.000237",
    #   "2018-02-16 15:32:06.023518-#general": "1518784325.000324",
    #   "2018-02-16 15:32:07.285910-#general": "1518784327.000207",
    #   "message_unique_key-#general": "1518784328.000029"
    #  },
    #  "channel_ids": {
    #   "#general": "C0NKNEZ89"
    #  }
    # }"""
    #
    #     send_attachment_message()
    #     send_general_slack_message_with_attachments()
    message = SimpleMessage(text='hello world')
    messenger.upd_message(message, channel='#hub')
    messenger.upd_message(message, channel='#log')
