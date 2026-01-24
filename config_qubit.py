"""
Config for basic transmon manipulation and readout
"""

from qualang_tools.units import unit
import numpy as np
u = unit(coerce_to_integer=True)

#############################################
#                  Qubit                    #
#############################################
qubit_IF = 50 * u.MHz
qubit_LO =  3868.18 * u.MHz - qubit_IF
qubit_LO_gain = 6

# Continuous wave
const_len = 100
const_amp = 0.1
# Saturation_pulse
saturation_len = 20 * u.us
saturation_amp = 0.1
# Square pi pulse
square_pi_len = 112 * u.ns
square_pi_amp = 0.125
# Gaussian pulses
gaussian_pulse = lambda amplitude, length, sigma : amplitude * np.exp(-(np.arange(length)-length/2)** 2/(2*sigma**2))
rot_180_len = 248
rot_180_sigma = rot_180_len / 5
rot_180_amp = 0.125
x180_I_wf = gaussian_pulse(rot_180_amp, rot_180_len, rot_180_sigma)
x180_Q_wf = np.zeros(rot_180_len)
y180_I_wf = np.zeros(rot_180_len)
y180_Q_wf = gaussian_pulse(rot_180_amp, rot_180_len, rot_180_sigma)

rot_90_len = rot_180_len
rot_90_sigma = rot_180_sigma
rot_90_amp = rot_180_amp / 2
x90_I_wf = gaussian_pulse(rot_90_amp, rot_90_len, rot_90_sigma)
x90_Q_wf = np.zeros(rot_90_len)
y90_I_wf = np.zeros(rot_90_len)
y90_Q_wf = gaussian_pulse(rot_90_amp, rot_90_len, rot_90_sigma)

#############################################
#                Resonators                 #
#############################################
resonator_IF = 60 * u.MHz
resonator_LO = 5930 *u.MHz - resonator_IF
resonator_LO_gain = 7

readout_len = 2000
readout_delay = 0 # Skip the first points
readout_amp = 0.125

time_of_flight = 256 * u.ns
depletion_time = 2 * u.us


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
            "digital_outputs": {1: {}, 2: {}, 3: {}},
            "analog_inputs": {
                1: {"offset": 0.012341 -0.002883, "gain_db": -3},  # I from down-conversion
                2: {"offset": 0.009397 -0.002604, "gain_db": -3},  # Q from down-conversion
            },
        }
    },
    "elements": {
        "qubit": {
            "RF_inputs": {"port": ("oct1", 2)},
            'digitalInputs': {
            'trigger': {
                'port': ('con1', 2),
                'delay': 57,
                'buffer': 18,
               }
            },
            "intermediate_frequency": qubit_IF,
            "operations": {
                "cw": "const_pulse",
                "saturation": "saturation_pulse",
                "pi": "square_pi_pulse",
                "pi_half": "square_pi_half_pulse",
                "x90": "x90_pulse",
                "x180": "x180_pulse",
                "y90": "y90_pulse",
                "y180": "y180_pulse",
            },
        },
        "resonator": {
            "RF_inputs": {"port": ("oct1", 1)},
            "RF_outputs": {"port": ("oct1", 1)},
            'digitalInputs': {
            'trigger': {
                'port': ('con1', 1),
                'delay': 57,
                'buffer': 18,
               }
            },
            "intermediate_frequency": resonator_IF,
            "operations": {
                "cw": "const_pulse",
                "readout": "readout_pulse",
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
                    "LO_frequency": resonator_LO,
                    "LO_source": "internal",
                    "output_mode": "always_on",
                    "gain": resonator_LO_gain,
                },
                2: {
                    "LO_frequency": qubit_LO,
                    "LO_source": "internal",
                    "output_mode": "always_on",
                    "gain": qubit_LO_gain,
                },
            },
            "RF_inputs": {
                1: {
                    "LO_frequency": resonator_LO,
                    "LO_source": "internal",
                },
            },
            "connectivity": "con1",
        }
    },
    "pulses": {
        "const_pulse": {
            "operation": "control",
            "length": const_len,
            "waveforms": {
                "I": "const_wf",
                "Q": "zero_wf",
            },
            'digital_marker': 'ON'
        },
        "square_pi_pulse": {
            "operation": "control",
            "length": square_pi_len,
            "waveforms": {
                "I": "square_pi_wf",
                "Q": "zero_wf",
            },
            'digital_marker': 'ON'
        },
        "square_pi_half_pulse": {
            "operation": "control",
            "length": square_pi_len,
            "waveforms": {
                "I": "square_pi_half_wf",
                "Q": "zero_wf",
            },
            'digital_marker': 'ON'
        },
        "saturation_pulse": {
            "operation": "control",
            "length": saturation_len,
            "waveforms": {"I": "saturation_drive_wf", "Q": "zero_wf"},
            'digital_marker': 'ON'
        },
        "x90_pulse": {
            "operation": "control",
            "length": rot_90_len,
            "waveforms": {
                "I": "x90_I_wf",
                "Q": "x90_Q_wf",
            },
            'digital_marker': 'ON'
        },
        "x180_pulse": {
            "operation": "control",
            "length": rot_180_len,
            "waveforms": {
                "I": "x180_I_wf",
                "Q": "x180_Q_wf",
            },
            'digital_marker': 'ON'
        },
        "y90_pulse": {
            "operation": "control",
            "length": rot_90_len,
            "waveforms": {
                "I": "y90_I_wf",
                "Q": "y90_Q_wf",
            },
            'digital_marker': 'ON'
        },
        "y180_pulse": {
            "operation": "control",
            "length": rot_180_len,
            "waveforms": {
                "I": "y180_I_wf",
                "Q": "y180_Q_wf",
            },
            'digital_marker': 'ON'
        },
        "readout_pulse": {
            "operation": "measurement",
            "length": readout_len,
            "waveforms": {
                "I": "readout_wf",
                "Q": "zero_wf",
            },
            "integration_weights": {
                "cos": "cosine_weights",
                "sin": "sine_weights",
                "minus_sin": "minus_sine_weights",
            },
            "digital_marker": "ON",
        },
        "trigger_pulse" : { "operation" : "control", "length": 100, "digital_marker":"ON"},
    },
    "waveforms": {
        "const_wf": {"type": "constant", "sample": const_amp},
        "saturation_drive_wf": {"type": "constant", "sample": saturation_amp},
        "square_pi_wf": {"type": "constant", "sample": square_pi_amp},
        "square_pi_half_wf": {"type": "constant", "sample": square_pi_amp / 2},
        "zero_wf": {"type": "constant", "sample": 0.0},
        "x90_I_wf": {"type": "arbitrary", "samples": x90_I_wf.tolist()},
        "x90_Q_wf": {"type": "arbitrary", "samples": x90_Q_wf.tolist()},
        "x180_I_wf": {"type": "arbitrary", "samples": x180_I_wf.tolist()},
        "x180_Q_wf": {"type": "arbitrary", "samples": x180_Q_wf.tolist()},
        "y90_Q_wf": {"type": "arbitrary", "samples": y90_Q_wf.tolist()},
        "y90_I_wf": {"type": "arbitrary", "samples": y90_I_wf.tolist()},
        "y180_Q_wf": {"type": "arbitrary", "samples": y180_Q_wf.tolist()},
        "y180_I_wf": {"type": "arbitrary", "samples": y180_I_wf.tolist()},
        "readout_wf": {"type": "constant", "sample": readout_amp},
    },
    "digital_waveforms": {
        "ON": {"samples": [(1, 0)]},
    },
    "integration_weights": {
        "cosine_weights": {
            "cosine": [(0.0,readout_delay),(1.0, readout_len-readout_delay)],
            "sine": [(0.0, readout_len)],
        },
        "sine_weights": {
            "cosine": [(0.0, readout_len)],
            "sine": [(0.0,readout_delay),(1.0, readout_len-readout_delay)],
        },
        "minus_sine_weights": {
            "cosine": [(0.0, readout_len)],
            "sine": [(0.0,readout_delay),(-1.0, readout_len-readout_delay)],
        },
    },
}
with open('config_qubit.md','w') as f:
    f.write(f"""## Recap of the config

### Qubit

- qubit_LO = {qubit_LO/1e6:.1f} MHz
- qubit_IF = {qubit_IF/1e6:.1f} MHz
- qubit_LO_gain = {qubit_LO_gain} dB

#### Long square "saturation" pulse (`saturation`)

- saturation_len = {saturation_len}
- saturation_amp = {saturation_amp}

#### Square pulse (`pi`, `pi_half`)

- square_pi_len = {square_pi_len}
- square_pi_amp = {square_pi_amp}  
- square_pi_half_amp = {square_pi_amp/2}

#### Gaussian pulses (`x180`, `y180`, `x90`, `y90`)

- rot_180_len = {rot_180_len}  
- rot_180_sigma = {rot_180_sigma}
- rot_180_amp = {rot_180_amp} 
- rot_90_len = {rot_90_len}  
- rot_90_sigma = {rot_90_sigma}
- rot_90_amp = {rot_90_amp}  

### Readout Resonator

- resonator_LO = {resonator_LO/1e6:.1f} MHz  
- resonator_IF = {resonator_IF/1e6:.1f} MHz
- resonator_LO_gain = {resonator_LO_gain} dB

#### Readout pulse (`readout`)

- readout_len = {readout_len}
- readout_delay = {readout_delay} 
- readout_amp = {readout_amp}
- time_of_flight = {time_of_flight}""")
