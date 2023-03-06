import time
import azure.cognitiveservices.speech as speechsdk
from constants import AZ_SUBSCRIPTION_KEY,AZ_LOCATION_ID
#CREDIT-
# https://github.com/Azure-Samples/cognitive-services-speech-sdk/blob/master/samples/python/console/speech_sample.py
def speech_recognize_continuous_from_microphone():
    """performs async speech recognition with input from microphone"""
    speech_config = speechsdk.SpeechConfig(subscription=AZ_SUBSCRIPTION_KEY, region=AZ_LOCATION_ID)
    audio_config = speechsdk.audio.AudioConfig(use_default_microphone=True)
    speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_config)
    completed = False # as we are not yet started

    def stop_cb(event):
        """callback that stops continuous recognition upon receiving an event """
        print('CLOSING on {}'.format(event))
        nonlocal completed
        completed = True

    # connect callbacks to the events fired by the speech recognizer
    speech_recognizer.session_started.connect(lambda event: print('SESSION STARTED: {}'.format(event)))
    speech_recognizer.session_stopped.connect(lambda event: print('\nSESSION STOPPED {}'.format(event)))
    speech_recognizer.recognized.connect(lambda event: print('\n{}'.format(event.result.text)))
    # stop continuous recognition on either session stopped or canceled events
    speech_recognizer.session_stopped.connect(stop_cb)
    speech_recognizer.canceled.connect(stop_cb)

    print("continuous recognition is running, say something!!!")
    result = speech_recognizer.start_continuous_recognition_async()
    

    #transcription_result.get()

    while not completed:
        time.sleep(.5)
        print("stopping async recognition!!!")
        break
    speech_recognizer.stop_continuous_recognition_async()
    
    # return out
