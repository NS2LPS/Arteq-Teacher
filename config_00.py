"""
QUA-Config supporting OPX+ & Octave
"""

from qualang_tools.units import unit
import numpy as np

#######################
# AUXILIARY FUNCTIONS #
#######################
u = unit(coerce_to_integer=True)

#############################################
#       Experimental Parameters             #
#############################################
qubit_LO = 10. * u.GHz
qubit_IF = 50. * u.MHz

readout_LO = 10. *u.GHz
readout_IF = 50. * u.MHz

# Pulse length
pulse_len = 60 * u.ns
pulse_amp = 0.1  # Keep this value between -0.125 and +0.125

# Readout length
readout_len = 800 * u.ns
short_readout_len = 80 * u.ns

# Time of flight
time_of_flight = 24

# Gaussian pulse
gaussian = lambda amplitude, length, sigma : amplitude * np.exp(-(np.arange(length)-length/2)** 2/(2*sigma**2)) 
gaussian_wf = gaussian(0.1, 200, 40)

#############################################
#                  Config                   #
#############################################
config = {
    "controllers": {
        "con1": {
            "analog_outputs": {
                1: {"offset": 0.0},  # I resonator
                2: {"offset": 0.0},  # Q resonator
                3: {"offset": 0.0},  # I qubit
                4: {"offset": 0.0},  # Q qubit
            },
            "digital_outputs": {3:{}},
            "analog_inputs": {
                1: {"offset": 0.012335, "gain_db": 0},  # I from down-conversion
                2: {"offset": 0.009358, "gain_db": 0},  # Q from down-conversion
            },
        }
    },
    "elements": {
        "qubit": {
            "RF_inputs": {"port": ("oct1", 2)},
            "intermediate_frequency": qubit_IF,
            "operations": {
                "pulse": "const_pulse", "gaussian":"gaussian_pulse"
            },
        },
        "scope": {
            "RF_inputs": {"port": ("oct1", 1)},
            "RF_outputs": {"port": ("oct1", 1)},
            "intermediate_frequency": readout_IF,
            "operations": {
                "cw": "const_pulse",
                "readout": "readout_pulse",
                "short_readout": "short_readout_pulse",          
            },
            "time_of_flight": time_of_flight,
            "smearing": 0,
        },
         "trigger": {
            "digitalInputs": { "trigger_in" : {"port": ("con1",3), "delay":0, "buffer":0 }},
            "operations" : {"trigger":"trigger_pulse"},  
        },
    },
    "octaves": {
        "oct1": {
            "RF_outputs": {
                1: {
                    "LO_frequency": readout_LO,
                    "LO_source": "internal",
                    "output_mode": "always_on",
                    "gain": -10,
                },
                2: {
                    "LO_frequency": qubit_LO,
                    "LO_source": "internal",
                    "output_mode": "always_on",
                    "gain": -10,
                },
            },
            "RF_inputs": {
                1: {
                    "LO_frequency": readout_LO,
                    "LO_source": "internal",
                },
            },
            "connectivity": "con1",
        }
    },
    "pulses": {
        "const_pulse": {
            "operation": "control",
            "length": pulse_len,
            "waveforms": {
                "I": "const_wf",
                "Q": "zero_wf",
            },
        },
        "gaussian_pulse":{
            "operation":"control",
            "length":200,
            "waveforms":{
                "I":"gaussian_wf",
                "Q":"zero_wf",
            }
        },
        "readout_pulse": {
            "operation": "measurement",
            "length": readout_len,
            "waveforms": {
                "I": "zero_wf",
                "Q": "zero_wf",
            },
            "integration_weights": {
                "cos": "cosine_weights",
                "sin": "sine_weights",
                "minus_sin": "minus_sine_weights",
            },
            "digital_marker": "ON",
        },
        "short_readout_pulse": {
            "operation": "measurement",
            "length": short_readout_len,
            "waveforms": {
                "I": "zero_wf",
                "Q": "zero_wf",
            },
            "integration_weights": {
                "cos": "cosine_weights",
                "sin": "sine_weights",
                "minus_sin": "minus_sine_weights",
            },
            "digital_marker": "ON",
        },
        "trigger_pulse" : { "operation":"control", "length": 100, "digital_marker":"ON"},
    },
    "waveforms": {
        "const_wf": {"type": "constant", "sample": pulse_amp},
        "zero_wf": {"type": "constant", "sample": 0.0},
        "gaussian_wf": {"type": "arbitrary", "samples": gaussian_wf.tolist()},
    },
    "digital_waveforms": {
        "ON": {"samples": [(1, 0)]},
    },
    "integration_weights": {
        "cosine_weights": {
            "cosine": [(1.0, readout_len)],
            "sine": [(0.0, readout_len)],
        },
        "sine_weights": {
            "cosine": [(0.0, readout_len)],
            "sine": [(1.0, readout_len)],
        },
        "minus_sine_weights": {
            "cosine": [(0.0, readout_len)],
            "sine": [(-1.0, readout_len)],
        },
    },
}
