"""RF front-end for multi-frequency capture (Phases 7–8).

Modules:
  * ``multiplex``   — run N per-channel dsd-fme decoders into one pipeline
  * ``channelizer`` — wideband IQ → per-channel narrowband FM audio (numpy)
  * ``capture``     — SoapySDR wideband IQ source (guarded import)
  * ``scheduler``   — decoder allocation / traffic-following (Phase 8)
  * ``energy``      — FFT per-channel power detection (Phase 8)

The DSP and hardware-open paths import numpy / SoapySDR lazily so the
rest of the package (channel plumbing, subprocess orchestration,
scheduling policy) imports and unit-tests without either installed.
"""
