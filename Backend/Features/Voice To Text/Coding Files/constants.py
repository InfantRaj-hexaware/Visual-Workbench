import pyaudio


# CONSTANTS --> AUDIO SETTINGS
FRAMES_PER_BUFFER = 5000
FORMAT = pyaudio.paInt16
CHANNELS = 2
FRAME_RATE = 20000
SECONDS = 30

# CONSTANT --> OUTPUT FILE NAME
RECORDING_OUTPUT_FILE_NAME = "output_recording.wav"
TRANSCRIPT_OUTPUT_FILE_NAME = "save_transcript.txt"


# CONSTANT --> ASSEMBLY.AI API KEY
SUBSCRIPTION_KEY = "a0ba86dc6a004985b38c9f3039908670"
UPLOAD_ENDPOINT = "https://api.assemblyai.com/v2/upload"
TRANSCRIPT_ENDPOINT = "https://api.assemblyai.com/v2/transcript"

# CONSTANT --> AZURE SPEECH API KEY
AZ_SUBSCRIPTION_KEY = "b0050fa2fd2a4ed59c01f3bbd915b376"
AZ_LOCATION_ID = "centralindia"

# CONSTANT --> DETECT HEADERS AND CONTENT
HEADERS_AUTH_ONLY = {'authorization': SUBSCRIPTION_KEY}
HEADERS = {
    "authorization": SUBSCRIPTION_KEY,
    "content-type": "application/json"
}

CHUNK_SIZE = 5242880  # 5MB



# CONSTANT --> RECORDING FILE PATH
RECORDING_FILE_PATH = "D:\\CTO\\SpeechToText\\DebiTeam\\AzureModel\\recording_output"

# CONSTANT --> TRANSCRIPTION FILE PATH
TRANSCRIPTION_FILE_PATH = "D:\\CTO\\SpeechToText\\DebiTeam\\AzureModel\\transcription_output"



