# todo: retry on slack communication
# todo: use cache or not?
# todo: clean cache
# message structure: https://api.slack.com/docs/messages
# todo: make properly
# HARDCODE to implement retry logic inside Slacker reflexion

""" Send and update messages to slack transparently using message ids. """
import json
import os
from datetime import datetime
import requests
from retrypy import retry

import slacker
from slacker import *
from slacker import API_BASE_URL


@retry.decorate(requests.exceptions.RequestException, times=7, wait=lambda n: 2 ** n)
def _request(self, method, api, **kwargs):
    if self.token:
        kwargs.setdefault('params', {})['token'] = self.token

    response = method(API_BASE_URL.format(api=api),
                      timeout=self.timeout,
                      proxies=self.proxies,
                      **kwargs)

    response.raise_for_status()

    response = Response(response.text)
    if not response.successful:
        raise Error(response.error)

    return response


slacker.BaseAPI._request = _request


class Attachment(object):
    def __init__(self, **kwargs):
        self.attachment_text = None
        self.pretext = None
        self.fallback = None
        self.color = None
        self.title = None
        self.title_link = None
        self.author_name = None
        self.author_link = None
        self.author_icon = None
        self.footer = None
        self.footer_icon = None
        self.ts = None
        self.fields = []

        for k, v in kwargs.items():
            setattr(self, k, v)

    def add_field(self, title, value, short=True):
        self.fields.append({'title': title, 'value': value, 'short': short})

    def to_json(self):
        return {
            "fallback": self.fallback,
            "color": self.color,
            "title": self.title,
            "title_link": self.title_link,
            "author_name": self.author_name,
            "author_link": self.author_link,
            "author_icon": self.author_icon,
            "fields": self.fields,
            "footer": self.footer,
            "pretext": self.pretext,
            "text": self.attachment_text,
            'ts': self.ts
        }


class SlackMessage(object):
    def __init__(self, **kwargs):
        self.text = None
        self.attachments = []

        for k, v in kwargs.items():
            setattr(self, k, v)

        if not self.attachments:
            self.attachments = []

        if self.attachments:
            self.attachments = [self._cast_attachment(attachment) for attachment in self.attachments]

    def _cast_attachment(self, attachment):
        if isinstance(attachment, Attachment):
            return attachment
        elif isinstance(attachment, dict):
            return Attachment(**attachment)
        else:
            raise Exception('Unknown attachment type')

    def add_attachment(self, **kwargs):
        self.attachments.append(Attachment(**kwargs))

    def to_json(self):
        res = {
            "text": self.text,
            "attachments": [attachment.to_json() for attachment in self.attachments]
        }

        # clear unused fields
        if not self.text:
            res.pop('text')
        res['attachments'] = [{k: v for k, v in attachment.items() if v is not None} for attachment in res['attachments']]
        res['attachments'] = [attachment for attachment in res['attachments'] if attachment]
        if not res['attachments']:
            res.pop('attachments')

        return res

    def __str__(self):
        return str(self.to_json())

    def __repr__(self):
        return str(self.to_json())


class SimpleMessage(SlackMessage):
    """ Message with one attachment. """

    def __init__(self, **kwargs):
        super().__init__(text=kwargs.pop('text', None), attachments=None)
        self.add_attachment(**kwargs)

    def add_field(self, *args, **kwargs):
        self.attachments[0].add_field(*args, **kwargs)


class SlackMessenger(Slacker):
    def __init__(self, token):
        super().__init__(token)
        self.token = token
        # dict(<message_id>-<channel>: <ts>)
        self.message_timestamps = {}
        self.channel_ids = {}

        self.cache_fn = 'slack_messenger_cache.json'
        self.load_cache()

    def load_cache(self):
        if os.path.exists(self.cache_fn):
            with open(self.cache_fn, 'r') as f:
                self.cache = json.load(f)

            if self.token != self.cache.get('token'):
                # reset cache when slack changes
                self.cache = {}
            else:
                self.message_timestamps = self.cache['message_timestamps']
                self.channel_ids = self.cache['channel_ids']
        else:
            self.cache = {}

    def dump_cache(self):
        self.cache['message_timestamps'] = self.message_timestamps
        self.cache['channel_ids'] = self.channel_ids
        self.cache['token'] = self.token

        with open(self.cache_fn, 'w') as f:
            json.dump(self.cache, f, indent=1)

    def upd_message(self, slack_message, channel, message_id=None):
        # Update message if it exists. Otherwise, post a new message
        """
        :param slack_message: `SlackMessage` or dict or str. In the latter case, str will be used as a text of the message
        :param message_id: key of the message. If the key is the same in 2 consecutive calls, the latter message will be updated. If not specified, the unique message id will be generated
        :param channel: slack channel
        :return:
        """
        if not message_id:
            message_id = str(datetime.now())

        full_id = "{}-{}".format(message_id, channel)

        try:
            # get channel_id
            if channel in self.channel_ids:
                channel_id = self.channel_ids[channel]
            else:
                channel_id = self.get_channel_id_by_name(channel)
                self.channel_ids[channel] = channel_id
        except:
            # channel not found. Possibly, channel is a direct user link (@username)
            channel_id = channel

        # convert slack_message to dict
        if isinstance(slack_message, SlackMessage):
            message_js = slack_message.to_json()
        elif isinstance(slack_message, dict):
            message_js = slack_message
        elif isinstance(slack_message, str):
            message_js = SlackMessage(text=slack_message).to_json()
        else:
            raise Exception('Slack message should be one of: SlackMessage, dict, str')

        message_ts = self.message_timestamps.get(full_id)

        if not message_ts:
            res = self.chat.post_message(text=message_js.get('text'), channel=channel_id, attachments=message_js.get('attachments'))
            self.message_timestamps[full_id] = res.body['ts']
            self.dump_cache()
        else:
            try:
                self.chat.update(text=message_js.get('text'), channel=channel_id, attachments=message_js.get('attachments'), ts=message_ts)
            except Exception as e:
                if str(e) == 'message_not_found':
                    res = self.chat.post_message(text=message_js.get('text'), channel=channel_id, attachments=message_js.get('attachments'))
                    self.message_timestamps[full_id] = res.body['ts']
                    self.dump_cache()
                else:
                    # todo: handle properly
                    print("Unknown error    !", e)

    def get_channels(self):
        return self.channels.list().body['channels']

    def get_channel_id_by_name(self, channel_name):
        if channel_name[0] == '#':
            channel_name = channel_name[1:]
        channels = self.get_channels()
        channel = [ch for ch in channels if ch['name_normalized'] == channel_name][0]
        return channel['id']
