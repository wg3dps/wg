#!/usr/bin/env python3
# ==============================================================================
# 更新时间：2026-05-05
# 文件说明：openpilot 控制系统主模块
# 更新内容：场景化纵向智控：低定速/弯道/碰撞预警自动切换 OP、静止模式可选、弯道文字提示
# 作    者：OPENPILOT 珠海佬
# 微信号：Bibibibalalala
# 添加微信好友时请备注来源
# ==============================================================================
import os

import math
import time
from typing import SupportsFloat

from cereal import car, log
from openpilot.common.numpy_fast import clip, interp
from openpilot.common.realtime import config_realtime_process, Priority, Ratekeeper, DT_CTRL, DT_MDL
from openpilot.common.profiler import Profiler
from openpilot.common.params import Params, put_nonblocking, put_bool_nonblocking
import cereal.messaging as messaging
from cereal.visionipc import VisionIpcClient, VisionStreamType
from openpilot.common.conversions import Conversions as CV
from panda import ALTERNATIVE_EXPERIENCE
from openpilot.system.swaglog import cloudlog
from openpilot.system.version import get_short_branch
from openpilot.selfdrive.boardd.boardd import can_list_to_can_capnp
from openpilot.selfdrive.car.car_helpers import get_car, get_startup_event, get_one_can
from openpilot.selfdrive.controls.lib.lateral_planner import CAMERA_OFFSET
from openpilot.selfdrive.controls.lib.drive_helpers import VCruiseHelper, get_lag_adjusted_curvature, CONTROL_N
from openpilot.selfdrive.controls.lib.latcontrol import LatControl, MIN_LATERAL_CONTROL_SPEED
from openpilot.selfdrive.controls.lib.longcontrol import LongControl
from openpilot.selfdrive.controls.lib.longcontrol_tuner import LongControlTuner
from openpilot.selfdrive.controls.lib.latcontrol_pid import LatControlPID
from openpilot.selfdrive.controls.lib.latcontrol_indi import LatControlINDI
from openpilot.selfdrive.controls.lib.latcontrol_lqr import LatControlLQR
from openpilot.selfdrive.controls.lib.latcontrol_angle import LatControlAngle, STEER_ANGLE_SATURATION_THRESHOLD
from openpilot.selfdrive.controls.lib.latcontrol_torque import LatControlTorque
from openpilot.selfdrive.controls.lib.events import Events, ET, CustomEventName
from openpilot.selfdrive.controls.lib.alertmanager import AlertManager, set_offroad_alert
from openpilot.selfdrive.controls.lib.vehicle_model import VehicleModel
from openpilot.system.hardware import TICI
from openpilot.selfdrive.hybrid_modeld.constants import ModelConstants



SOFT_DISABLE_TIME = 3
LDW_MIN_SPEED = 38 * CV.MPH_TO_MS  # 【可调】车道偏离预警速度（英里）
LANE_DEPARTURE_THRESHOLD = 0.1
IGNORE_PROCESSES = {"loggerd", "encoderd", "statsd", "mapd", "gpxd", "loggerd_wrapper", "deleter", "uploader"}
REPLAY = "REPLAY" in os.environ
SIMULATION = "SIMULATION" in os.environ
TESTING_CLOSET = "TESTING_CLOSET" in os.environ
NOSENSOR = "NOSENSOR" in os.environ

NO_IR_CTRL = Params().get_bool("dp_device_no_ir_ctrl")
if NO_IR_CTRL:
  IGNORE_PROCESSES |= {'driverCameraState', 'driverMonitoringState'}

ThermalStatus = log.DeviceState.ThermalStatus
State = log.ControlsState.OpenpilotState
PandaType = log.PandaState.PandaType
Desire = log.LateralPlan.Desire
LaneChangeState = log.LateralPlan.LaneChangeState
LaneChangeDirection = log.LateralPlan.LaneChangeDirection
EventName = car.CarEvent.EventName
ButtonType = car.CarState.ButtonEvent.Type
SafetyModel = car.CarParams.SafetyModel

IGNORED_SAFETY_MODES = (SafetyModel.silent, SafetyModel.noOutput)
CSID_MAP = {"1": EventName.roadCameraError, "2": EventName.wideRoadCameraError, "0": EventName.driverCameraError}
ACTUATOR_FIELDS = tuple(car.CarControl.Actuators.schema.fields.keys())
ACTIVE_STATES = (State.enabled, State.softDisabling, State.overriding)
ENABLED_STATES = (State.preEnabled, *ACTIVE_STATES)

DP_VAG_TIMEBOMB_BYPASS_WARNING = 34000
DP_VAG_TIMEBOMB_BYPASS_START = 345000
DP_VAG_TIMEBOMB_BYPASS_END = 348000
CONTROL_N_T_IDX=ModelConstants.T_IDXS[:CONTROL_N]

def get_accel_from_plan(CP, speeds, accels):
    if len(speeds) == CONTROL_N and len(accels) == CONTROL_N:
      v_target_now = interp(DT_MDL, CONTROL_N_T_IDX, speeds)
      a_target_now = interp(DT_MDL, CONTROL_N_T_IDX, accels)
      delay = (CP.longitudinalActuatorDelayLowerBound + CP.longitudinalActuatorDelayUpperBound) * 0.5
      v_target = interp(delay + DT_MDL, CONTROL_N_T_IDX, speeds)
      a_target = 2 * (v_target - v_target_now) / delay - a_target_now

    else:
      v_target = 0.0
      v_target_now = 0.0
      a_target = 0.0
    return a_target
  
class Controls:
  def __init__(self, sm=None, pm=None, can_sock=None, CI=None):
    config_realtime_process(4 if TICI else 3, Priority.CTRL_HIGH)

    self.dp_gps_ok_once = False

    self.branch = get_short_branch("")

    self.pm = pm
    if self.pm is None:
      self.pm = messaging.PubMaster(['sendcan', 'controlsState', 'carState',
                                     'carControl', 'carEvents', 'carParams', 'controlsStateExt'])

    if NO_IR_CTRL:
      self.camera_packets = ["roadCameraState"]
    else:
      self.camera_packets = ["roadCameraState", "driverCameraState"]

    can_timeout = None if os.environ.get('NO_CAN_TIMEOUT', False) else 20
    self.can_sock = messaging.sub_sock('can', timeout=can_timeout)

    self.log_sock = messaging.sub_sock('androidLog')

    self.params = Params()
    self.dp_no_gps_ctrl = self.params.get_bool("dp_no_gps_ctrl")
    self.dp_no_fan_ctrl = self.params.get_bool("dp_no_fan_ctrl")
    self.dp_0813 = self.params.get_bool("dp_0813")
    self._dp_alka = self.params.get_bool("dp_alka")
    self._dp_alka_active = True
    self._dp_alka_trigger_count = 0
    self._dp_alka_btn_block_frame = 0
    self.dp_device_disable_temp_check = self.params.get_bool("dp_device_disable_temp_check")
    self._dp_vag_timebomb_bypass_counter = 0
    self._dp_vag_timebomb_bypass = self.params.get_bool("dp_vag_timebomb_bypass")
    self._dp_lat_lane_change_assist_disabled = int(self.params.get("dp_lat_lane_change_assist_speed", encoding="utf-8")) == 0
    self._dp_lat_lane_change_assist_disabled_active = False
    self._dp_lane_change_event_triggered = False
    self.torqued_override = self.params.get_bool("CustomTorqueLateral")
    
    self.sm = sm
    if self.sm is None:
      ignore = ['testJoystick']
      if SIMULATION:
        ignore += ['driverCameraState', 'managerState']
      if NO_IR_CTRL:
        ignore += ['driverCameraState', 'driverMonitoringState']
      self.sm = messaging.SubMaster(['deviceState', 'pandaStates', 'peripheralState', 'modelV2', 'liveCalibration',
                                     'driverMonitoringState', 'longitudinalPlan', 'lateralPlan', 'liveLocationKalman',
                                     'managerState', 'liveParameters', 'radarState', 'liveTorqueParameters', 'testJoystick'] + self.camera_packets,
                                    ignore_alive=ignore, ignore_avg_freq=['radarState', 'testJoystick'])

    if CI is None:
      get_one_can(self.can_sock)
      num_pandas = len(messaging.recv_one_retry(self.sm.sock['pandaStates']).pandaStates)
      experimental_long_allowed = self.params.get_bool("ExperimentalLongitudinalEnabled")
      self.CI, self.CP = get_car(self.can_sock, self.pm.sock['sendcan'], experimental_long_allowed, num_pandas)
    else:
      self.CI, self.CP = CI, CI.CP

    self.joystick_mode = self.params.get_bool("JoystickDebugMode") or self.CP.notCar

    self.disengage_on_accelerator = self.params.get_bool("DisengageOnAccelerator")
    self.CP.alternativeExperience = 0
    if not self.disengage_on_accelerator:
      self.CP.alternativeExperience |= ALTERNATIVE_EXPERIENCE.DISABLE_DISENGAGE_ON_GAS
    if self._dp_alka:
      self.CP.alternativeExperience |= ALTERNATIVE_EXPERIENCE.ALKA
    
    self.is_metric = self.params.get_bool("IsMetric")
    self.is_ldw_enabled = self.params.get_bool("IsLdwEnabled")
    openpilot_enabled_toggle = self.params.get_bool("OpenpilotEnabledToggle")
    passive = self.params.get_bool("Passive") or not openpilot_enabled_toggle


    car_recognized = self.CP.carName != 'mock'

    controller_available = self.CI.CC is not None and not passive and not self.CP.dashcamOnly
    self.read_only = not car_recognized or not controller_available or self.CP.dashcamOnly
    if self.read_only:
      safety_config = car.CarParams.SafetyConfig.new_message()
      safety_config.safetyModel = car.CarParams.SafetyModel.noOutput
      self.CP.safetyConfigs = [safety_config]

    prev_cp = self.params.get("CarParamsPersistent")
    if prev_cp is not None:
      self.params.put("CarParamsPrevRoute", prev_cp)

    cp_bytes = self.CP.to_bytes()
    self.params.put("CarParams", cp_bytes)
    put_nonblocking("CarParamsCache", cp_bytes)
    put_nonblocking("CarParamsPersistent", cp_bytes)

    if not self.CP.experimentalLongitudinalAvailable:
      self.params.remove("ExperimentalLongitudinalEnabled")
    if not self.CP.openpilotLongitudinalControl:
      self.params.remove("ExperimentalMode")

    self.CC = car.CarControl.new_message()
    self.CS_prev = car.CarState.new_message()
    self.AM = AlertManager()
    self.events = Events()

    if self.CP.useLongitudinalTuner:
      self.LoC = LongControlTuner(self.CP)
    else:
      self.LoC = LongControl(self.CP)
    self.VM = VehicleModel(self.CP)

    self.LaC: LatControl
    if self.CP.steerControlType == car.CarParams.SteerControlType.angle:
      self.LaC = LatControlAngle(self.CP, self.CI)
    elif self.CP.lateralTuning.which() == 'pid':
      self.LaC = LatControlPID(self.CP, self.CI)
    elif self.CP.lateralTuning.which() == 'indi':
      self.LaC = LatControlINDI(self.CP, self.CI)
    elif self.CP.lateralTuning.which() == 'lqr':
      self.LaC = LatControlLQR(self.CP, self.CI)
    elif self.CP.lateralTuning.which() == 'torque':
      self.LaC = LatControlTorque(self.CP, self.CI)

    self.initialized = False
    self.state = State.disabled
    self.enabled = False
    self.active = False
    self.soft_disable_timer = 0
    self.mismatch_counter = 0
    self.cruise_mismatch_counter = 0
    self.can_rcv_timeout_counter = 0
    self.can_rcv_cum_timeout_counter = 0
    self.last_blinker_frame = 0
    self.last_steering_pressed_frame = 0
    self.distance_traveled = 0
    self.last_functional_fan_frame = 0
    self.events_prev = []
    self.current_alert_types = [ET.PERMANENT]
    self.logged_comm_issue = None
    self.not_running_prev = None
    self.last_actuators = car.CarControl.Actuators.new_message()
    self.steer_limited = False
    self.desired_curvature = 0.0
    self.desired_curvature_rate = 0.0
    self.experimental_mode = False
    self.force_experimental_mode = False
    self.resume_filter_count = 0
    self.v_cruise_helper = VCruiseHelper(self.CP)
    self.recalibrating_seen = False
    
    self._lead_was_stopped = False
    self._lead_started_detected = False
    self._lead_started_sent = False
    self._lead_stop_d_rel = 0
    self._lead_stationary_alert_sent = False
    self._lead_approach_alert_sent = False
    self._lead_brake_alert_sent = False
    self._lead_status = False
    self._lead_status_timer = 0
    self._in_curve = False
    self._curve_exit_timer = 0
    self._adaptive_accel_enabled = True  # 增强型自适应跟车开关
    
    self.sm['liveParameters'].valid = True
    self.can_log_mono_time = 0

    self.startup_event = get_startup_event(car_recognized, controller_available, len(self.CP.carFw) > 0)


    if not car_recognized:
      self.events.add(EventName.carUnrecognized, static=True)
      if len(self.CP.carFw) > 0:
        set_offroad_alert("Offroad_CarUnrecognized", True)
      else:
        set_offroad_alert("Offroad_NoFirmware", True)
    elif self.read_only:
      self.events.add(EventName.dashcamMode, static=True)
    elif self.joystick_mode:
      self.events.add(EventName.joystickDebug, static=True)
      self.startup_event = None

    self.rk = Ratekeeper(100, print_delay_threshold=None)
    self.prof = Profiler(False)

  def set_initial_state(self):
    if REPLAY:
      controls_state = Params().get("ReplayControlsState")
      if controls_state is not None:
        controls_state = log.ControlsState.from_bytes(controls_state)
        self.v_cruise_helper.v_cruise_kph = controls_state.vCruise
      if any(ps.controlsAllowed for ps in self.sm['pandaStates']):
        self.state = State.enabled

  def update_events(self, CS):
    """Compute carEvents from carState"""
    self.events.clear()

    if self.startup_event is not None:
      self.events.add(self.startup_event)
      self.startup_event = None

    if not self.initialized:
      self.events.add(EventName.controlsInitializing)
      return

    if self.read_only:
      return

    if self._dp_alka and CS.brakePressed:
      if self.CP.pcmCruise and CS.cruiseState.available != self.CS_prev.cruiseState.available:
        self._dp_alka_trigger_count += 1
      if self._dp_alka_trigger_count == 2:
        self._dp_alka_active = not self._dp_alka_active
      if self.sm.frame % 50 == 0:
        self._dp_alka_trigger_count = 0
      if not self.CP.pcmCruise and self._dp_alka_btn_block_frame < self.sm.frame:
        if any(be.type in (ButtonType.decelCruise, ButtonType.setCruise) for be in CS.buttonEvents):
          self._dp_alka_active = not self._dp_alka_active
          self._dp_alka_btn_block_frame = self.sm.frame + 100

    resume_pressed = any(be.type in (ButtonType.accelCruise, ButtonType.resumeCruise) for be in CS.buttonEvents)
    if not self.CP.pcmCruise and not self.v_cruise_helper.v_cruise_initialized and resume_pressed:
      self.events.add(EventName.resumeBlocked)

    if (CS.gasPressed and not self.CS_prev.gasPressed and self.disengage_on_accelerator) or \
      (CS.brakePressed and (not self.CS_prev.brakePressed or not CS.standstill)) or \
      (CS.regenBraking and (not self.CS_prev.regenBraking or not CS.standstill)):
      self.events.add(EventName.pedalPressed)

    if CS.brakePressed and CS.standstill:
      self.events.add(EventName.preEnableStandstill)

    if CS.gasPressed:
      self.events.add(EventName.gasPressedOverride)

    if not self.CP.notCar and not NO_IR_CTRL:
      self.events.add_from_msg(self.sm['driverMonitoringState'].events)

    if CS.canValid:
      self.events.add_from_msg(CS.events)

    if not self.dp_device_disable_temp_check and self.sm['deviceState'].thermalStatus >= ThermalStatus.red:
      self.events.add(EventName.overheat)
    if self.sm['deviceState'].freeSpacePercent < 7 and not SIMULATION:
      self.events.add(EventName.outOfSpace)
    if self.sm['deviceState'].memoryUsagePercent > 90 and not SIMULATION:
      self.events.add(EventName.lowMemory)

    if not self.dp_no_fan_ctrl and self.sm['peripheralState'].pandaType != log.PandaState.PandaType.unknown:
      if self.sm['peripheralState'].fanSpeedRpm == 0 and self.sm['deviceState'].fanSpeedPercentDesired > 50:
        if (self.sm.frame - self.last_functional_fan_frame) * DT_CTRL > 15.0:
          self.events.add(EventName.fanMalfunction)
      else:
        self.last_functional_fan_frame = self.sm.frame

    cal_status = self.sm['liveCalibration'].calStatus
    if cal_status != log.LiveCalibrationData.Status.calibrated:
      if cal_status == log.LiveCalibrationData.Status.uncalibrated:
        self.events.add(EventName.calibrationIncomplete)
      elif cal_status == log.LiveCalibrationData.Status.recalibrating:
        if not self.recalibrating_seen:
          set_offroad_alert("Offroad_Recalibration", True)
        self.recalibrating_seen = True
        self.events.add(EventName.calibrationRecalibrating)
      else:
        self.events.add(EventName.calibrationInvalid)

    current_lane_change_state = self.sm['lateralPlan'].laneChangeState
    
    if self.sm['lateralPlan'].laneChangeState == LaneChangeState.preLaneChange:
      direction = self.sm['lateralPlan'].laneChangeDirection
      if (CS.leftBlindspot and direction == LaneChangeDirection.left) or \
         (CS.rightBlindspot and direction == LaneChangeDirection.right):
        self.events.add(EventName.laneChangeBlocked)
      else:
        if direction == LaneChangeDirection.left:
          self.events.add(EventName.preLaneChangeLeft)
        else:
          self.events.add(EventName.preLaneChangeRight)
      self._prev_lane_change_state = LaneChangeState.preLaneChange
      
    elif current_lane_change_state == LaneChangeState.laneChangeStarting:
      self.events.add(EventName.laneChange)
      if not hasattr(self, '_prev_lane_change_state'):
        self._prev_lane_change_state = LaneChangeState.off
      self._prev_lane_change_state = LaneChangeState.laneChangeStarting
      
    elif current_lane_change_state == LaneChangeState.laneChangeFinishing:
      self._prev_lane_change_state = LaneChangeState.laneChangeFinishing
      
    elif current_lane_change_state == LaneChangeState.off:
      self._prev_lane_change_state = LaneChangeState.off

    for i, pandaState in enumerate(self.sm['pandaStates']):
      if i < len(self.CP.safetyConfigs):
        safety_mismatch = pandaState.safetyModel != self.CP.safetyConfigs[i].safetyModel or \
                          pandaState.safetyParam != self.CP.safetyConfigs[i].safetyParam or \
                          pandaState.alternativeExperience != self.CP.alternativeExperience
      else:
        safety_mismatch = pandaState.safetyModel not in IGNORED_SAFETY_MODES

      if safety_mismatch or pandaState.safetyRxChecksInvalid or self.mismatch_counter >= 200:
        self.events.add(EventName.controlsMismatch)

      if log.PandaState.FaultType.relayMalfunction in pandaState.faults:
        self.events.add(EventName.relayMalfunction)

    num_events = len(self.events)

    not_running = {p.name for p in self.sm['managerState'].processes if not p.running and p.shouldBeRunning}
    if self.sm.rcv_frame['managerState'] and (not_running - IGNORE_PROCESSES):
      self.events.add(EventName.processNotRunning)
      if not_running != self.not_running_prev:
        cloudlog.event("process_not_running", not_running=not_running, error=True)
      self.not_running_prev = not_running
    else:
      if not SIMULATION and not self.rk.lagging:
        if not self.sm.all_alive(self.camera_packets):
          self.events.add(EventName.cameraMalfunction)
        elif not self.sm.all_freq_ok(self.camera_packets):
          self.events.add(EventName.cameraFrameRate)
    if len(self.sm['radarState'].radarErrors) or (not self.rk.lagging and not self.sm.all_checks(['radarState'])):
      self.events.add(EventName.radarFault)
    if not self.sm.valid['pandaStates']:
      self.events.add(EventName.usbError)
    if CS.canTimeout:
      self.events.add(EventName.canBusMissing)
    elif not CS.canValid:
      self.events.add(EventName.canError)

    can_rcv_timeout = self.can_rcv_timeout_counter >= 5
    has_disable_events = self.events.contains(ET.NO_ENTRY) and (self.events.contains(ET.SOFT_DISABLE) or self.events.contains(ET.IMMEDIATE_DISABLE))
    no_system_errors = (not has_disable_events) or (len(self.events) == num_events)
    if (not self.sm.all_checks() or can_rcv_timeout) and no_system_errors:
      if not self.sm.all_alive():
        self.events.add(EventName.commIssue)
      elif not self.sm.all_freq_ok():
        self.events.add(EventName.commIssueAvgFreq)
      else:
        self.events.add(EventName.commIssue)

      logs = {
        'invalid': [s for s, valid in self.sm.valid.items() if not valid],
        'not_alive': [s for s, alive in self.sm.alive.items() if not alive],
        'not_freq_ok': [s for s, freq_ok in self.sm.freq_ok.items() if not freq_ok],
        'can_rcv_timeout': can_rcv_timeout,
      }
      if logs != self.logged_comm_issue:
        cloudlog.event("commIssue", error=True, **logs)
        self.logged_comm_issue = logs
    else:
      self.logged_comm_issue = None

    if not self.sm['liveParameters'].valid and not TESTING_CLOSET and (not SIMULATION or REPLAY):
      self.events.add(EventName.vehicleModelInvalid)
    if not self.sm['lateralPlan'].mpcSolutionValid:
      self.events.add(EventName.plannerError)
    if not (self.sm['liveParameters'].sensorValid or self.sm['liveLocationKalman'].sensorsOK) and not NOSENSOR:
      if self.sm.frame > 5 / DT_CTRL:
        self.events.add(EventName.sensorDataInvalid)
    if not self.sm['liveLocationKalman'].posenetOK:
      self.events.add(EventName.posenetInvalid)
    if not self.sm['liveLocationKalman'].deviceStable:
      self.events.add(EventName.deviceFalling)

    if not REPLAY:
      cruise_mismatch = CS.cruiseState.enabled and (not self.enabled or not self.CP.pcmCruise)
      self.cruise_mismatch_counter = self.cruise_mismatch_counter + 1 if cruise_mismatch else 0
      if self.cruise_mismatch_counter > int(6. / DT_CTRL):
        self.events.add(EventName.cruiseMismatch)

    stock_long_is_braking = self.enabled and not self.CP.openpilotLongitudinalControl and CS.aEgo < -1.25
    model_fcw = self.sm['modelV2'].meta.hardBrakePredicted and not CS.brakePressed and not stock_long_is_braking
    planner_fcw = self.sm['longitudinalPlan'].fcw and self.enabled
    if planner_fcw or model_fcw:
      self.events.add(EventName.fcw)

    for m in messaging.drain_sock(self.log_sock, wait_for_one=False):
      try:
        msg = m.androidLog.message
        if any(err in msg for err in ("ERROR_CRC", "ERROR_ECC", "ERROR_STREAM_UNDERFLOW", "APPLY FAILED")):
          csid = msg.split("CSID:")[-1].split(" ")[0]
          evt = CSID_MAP.get(csid, None)
          if evt is not None:
            self.events.add(evt)
      except UnicodeDecodeError:
        pass

    if not SIMULATION or REPLAY:
      if not NOSENSOR and not self.dp_no_gps_ctrl:
        if not self.dp_gps_ok_once and self.sm['liveLocationKalman'].gpsOK:
          self.dp_gps_ok_once = True
        if self.dp_gps_ok_once and not self.sm['liveLocationKalman'].gpsOK and self.sm['liveLocationKalman'].inputsOK and (self.distance_traveled > 1500):
          self.events.add(EventName.noGps)
          if self.distance_traveled > 2000:
            self.dp_no_gps_ctrl = True
        if self.sm['liveLocationKalman'].gpsOK:
          self.distance_traveled = 0

      if self.sm['modelV2'].frameDropPerc > 20:
        self.events.add(EventName.modeldLagging)
      if self.sm['liveLocationKalman'].excessiveResets:
        self.events.add(EventName.localizerMalfunction)

  def data_sample(self):
    """Receive data from sockets and update carState"""
    can_strs = messaging.drain_sock_raw(self.can_sock, wait_for_one=True)
    CS = self.CI.update(self.CC, can_strs)
    if len(can_strs) and REPLAY:
      self.can_log_mono_time = messaging.log_from_bytes(can_strs[0]).logMonoTime

    self.sm.update(0)

    if not self.initialized:
      all_valid = CS.canValid and self.sm.all_checks()
      timed_out = self.sm.frame * DT_CTRL > (6. if REPLAY else 3.5)
      if all_valid or timed_out or (SIMULATION and not REPLAY):
        available_streams = VisionIpcClient.available_streams("camerad", block=False)
        if VisionStreamType.VISION_STREAM_ROAD not in available_streams:
          self.sm.ignore_alive.append('roadCameraState')
        if VisionStreamType.VISION_STREAM_WIDE_ROAD not in available_streams:
          self.sm.ignore_alive.append('wideRoadCameraState')

        if not self.read_only:
          self.CI.init(self.CP, self.can_sock, self.pm.sock['sendcan'])

        self.initialized = True
        self.set_initial_state()
        put_bool_nonblocking("ControlsReady", True)

    if not can_strs:
      self.can_rcv_timeout_counter += 1
      self.can_rcv_cum_timeout_counter += 1
    else:
      self.can_rcv_timeout_counter = 0

    if not self.enabled:
      self.mismatch_counter = 0

    if self.enabled and any(not ps.controlsAllowed for ps in self.sm['pandaStates']
           if ps.safetyModel not in IGNORED_SAFETY_MODES):
      self.mismatch_counter += 1

    self.distance_traveled += CS.vEgo * DT_CTRL

    return CS

  def state_transition(self, CS):
    """Compute conditional state transitions and execute actions on state transitions"""
    self.v_cruise_helper.update_v_cruise(CS, self.enabled, self.is_metric)
    self.soft_disable_timer = max(0, self.soft_disable_timer - 1)
    self.current_alert_types = [ET.PERMANENT]

    if self.state != State.disabled:
      if self.events.contains(ET.USER_DISABLE):
        self.state = State.disabled
        self.current_alert_types.append(ET.USER_DISABLE)

      elif self.events.contains(ET.IMMEDIATE_DISABLE):
        self.state = State.disabled
        self.current_alert_types.append(ET.IMMEDIATE_DISABLE)

      else:
        if self.state == State.enabled:
          if self.events.contains(ET.SOFT_DISABLE):
            self.state = State.softDisabling
            self.soft_disable_timer = int(SOFT_DISABLE_TIME / DT_CTRL)
            self.current_alert_types.append(ET.SOFT_DISABLE)
          elif self.events.contains(ET.OVERRIDE_LATERAL) or self.events.contains(ET.OVERRIDE_LONGITUDINAL):
            self.state = State.overriding
            self.current_alert_types += [ET.OVERRIDE_LATERAL, ET.OVERRIDE_LONGITUDINAL]

        elif self.state == State.softDisabling:
          if not self.events.contains(ET.SOFT_DISABLE):
            self.state = State.enabled
          elif self.soft_disable_timer > 0:
            self.current_alert_types.append(ET.SOFT_DISABLE)
          elif self.soft_disable_timer <= 0:
            self.state = State.disabled

        elif self.state == State.preEnabled:
          if not self.events.contains(ET.PRE_ENABLE):
            self.state = State.enabled
          else:
            self.current_alert_types.append(ET.PRE_ENABLE)

        elif self.state == State.overriding:
          if self.events.contains(ET.SOFT_DISABLE):
            self.state = State.softDisabling
            self.soft_disable_timer = int(SOFT_DISABLE_TIME / DT_CTRL)
            self.current_alert_types.append(ET.SOFT_DISABLE)
          elif not (self.events.contains(ET.OVERRIDE_LATERAL) or self.events.contains(ET.OVERRIDE_LONGITUDINAL)):
            self.state = State.enabled
          else:
            self.current_alert_types += [ET.OVERRIDE_LATERAL, ET.OVERRIDE_LONGITUDINAL]

    elif self.state == State.disabled:
      if self.events.contains(ET.ENABLE):
        if self.events.contains(ET.NO_ENTRY):
          self.current_alert_types.append(ET.NO_ENTRY)

        else:
          if self.events.contains(ET.PRE_ENABLE):
            self.state = State.preEnabled
          elif self.events.contains(ET.OVERRIDE_LATERAL) or self.events.contains(ET.OVERRIDE_LONGITUDINAL):
            self.state = State.overriding
          else:
            self.state = State.enabled
          self.current_alert_types.append(ET.ENABLE)
          self.v_cruise_helper.initialize_v_cruise(CS, self.experimental_mode)

    self.enabled = self.state in ENABLED_STATES
    self.active = self.state in ACTIVE_STATES
    if self.active or (self._dp_alka and self._dp_alka_active):
      self.current_alert_types.append(ET.WARNING)

  def state_control(self, CS):
    """Given the state, this function returns a CarControl packet"""
    lp = self.sm['liveParameters']
    x = max(lp.stiffnessFactor, 0.1)
    sr = max(lp.steerRatio, 0.1)
    self.VM.update_params(x, sr)

    if self.CP.lateralTuning.which() == 'torque':
      torque_params = self.sm['liveTorqueParameters']
      if self.sm.all_checks(['liveTorqueParameters']) and torque_params.useParams and not self.torqued_override:
        self.LaC.update_live_torque_params(torque_params.latAccelFactorFiltered, torque_params.latAccelOffsetFiltered,
                                           torque_params.frictionCoefficientFiltered)

    lat_plan = self.sm['lateralPlan']
    long_plan = self.sm['longitudinalPlan']
    model_v2 = self.sm['modelV2']

    CC = car.CarControl.new_message()
    CC.enabled = self.enabled
    MANUAL_CONTROL_MODE = 2  # 【开关】1=人机共驾模式 | 2=原版模式
    standstill = CS.vEgo <= max(self.CP.minSteerSpeed, MIN_LATERAL_CONTROL_SPEED) or CS.standstill
    blinker_engaged = CS.leftBlinker or CS.rightBlinker
    driver_override = CS.steeringPressed and self.active  
    recent_steering_pressed_short = (self.sm.frame - self.last_steering_pressed_frame) * DT_CTRL < 1.5  # 【可调】方向盘放松时间（秒）
    
    if MANUAL_CONTROL_MODE == 1:
      CC.latActive = self.active and not CS.steerFaultTemporary and not CS.steerFaultPermanent and \
                     (not standstill or self.joystick_mode) and \
                     not driver_override and not recent_steering_pressed_short
    else:
      CC.latActive = self.active and not CS.steerFaultTemporary and not CS.steerFaultPermanent and \
                     (not standstill or self.joystick_mode)
    
    CC.longActive = self.enabled and not self.events.contains(ET.OVERRIDE_LONGITUDINAL) and self.CP.openpilotLongitudinalControl

    if (self._dp_alka and self._dp_alka_active) and not standstill and CS.cruiseState.available:
      if self.sm['liveCalibration'].calStatus != log.LiveCalibrationData.Status.calibrated:
        pass
      elif CS.steerFaultTemporary or CS.steerFaultPermanent:
        pass
      elif CS.gearShifter == car.CarState.GearShifter.reverse:
        pass
      else:
        CC.latActive = True

    v_cruise_kph = self.v_cruise_helper.v_cruise_kph
    if v_cruise_kph < 29:  # 【可调】定速<29km/h 强制使用 OP 模式
      use_stock_acc = False
      self.force_experimental_mode = True
    else:
      self.force_experimental_mode = False  
      use_stock_acc = True

    # ========== 自车静止模式选择 ==========
    EGO_STOPPED_MODE = 1  # 【可调】1=使用原车ACC | 2=使用OP模式
    EGO_STOP_SPEED_KPH = 1.0  # 【可调】低于此值认为自车静止
    vehicle_stopped = CS.vEgo * CV.MS_TO_KPH < EGO_STOP_SPEED_KPH
    if hasattr(CS, 'standstill') and CS.standstill is not None:
      vehicle_stopped = vehicle_stopped or CS.standstill
    if vehicle_stopped:
      if EGO_STOPPED_MODE == 2:
        use_stock_acc = False

    if not hasattr(self, '_in_curve'):
        self._in_curve = False
    if not hasattr(self, '_curve_exit_timer'):
        self._curve_exit_timer = 0
    model_v2 = self.sm['modelV2']
    if hasattr(model_v2, 'orientationRate') and len(model_v2.orientationRate.z) > 0:
      omega_z = model_v2.orientationRate.z[0]
      if CS.vEgo > 1.0:
        curvature = abs(omega_z / CS.vEgo)
        if curvature > 0.003:  # 【可调】曲率>0.003 进入弯道模式
          use_stock_acc = False
          self._in_curve = True
          self._curve_exit_timer = 0
          self.events.add(CustomEventName.curveModeActive)
        else:
          if self._in_curve:
            self._curve_exit_timer += DT_CTRL
            if self._curve_exit_timer >= 1.5:  # 【可调】出弯后 1.5 秒过渡期
              self._in_curve = False
            else:
              use_stock_acc = False
    else:
      if self._in_curve:
        self._in_curve = False
        self._curve_exit_timer = 0

    lead_one = self.sm['radarState'].leadOne
    d_rel = lead_one.dRel if lead_one.status else 0
    
    # ========== 碰撞预警处理 ==========
    stock_long_is_braking = self.enabled and not self.CP.openpilotLongitudinalControl and CS.aEgo < -1.25
    if self.sm['radarState'].leadOne.fcw \
       or self.sm['radarState'].leadTwo.fcw \
       or self.sm['longitudinalPlan'].fcw \
       or (self.sm['modelV2'].meta.hardBrakePredicted and not CS.brakePressed and not stock_long_is_braking):
      use_stock_acc = False
    
    if self.CP.openpilotLongitudinalControl:
      CC.longActive = CC.longActive and not use_stock_acc



    if self._dp_lat_lane_change_assist_disabled:
      if not CS.leftBlinker and not CS.rightBlinker:
        if self._dp_lat_lane_change_assist_disabled_active:
          self._dp_lane_change_event_triggered = False
        self._dp_lat_lane_change_assist_disabled_active = False
      if not self._dp_lat_lane_change_assist_disabled_active and CS.steeringPressed and \
        ((CS.steeringTorque > 0 and CS.leftBlinker) or
         (CS.steeringTorque < 0 and CS.rightBlinker)):
        self._dp_lat_lane_change_assist_disabled_active = True
      if self._dp_lat_lane_change_assist_disabled_active:
        self.events.add(EventName.laneChange)
        CC.latActive = False

    if self._dp_vag_timebomb_bypass:
      if not CC.latActive:
        self._dp_vag_timebomb_bypass_counter = 0
      else:
        self._dp_vag_timebomb_bypass_counter += 1
        if DP_VAG_TIMEBOMB_BYPASS_WARNING <= self._dp_vag_timebomb_bypass_counter < DP_VAG_TIMEBOMB_BYPASS_START:
          self.events.add(EventName.steerTimeLimit)
        if self._dp_vag_timebomb_bypass_counter >= DP_VAG_TIMEBOMB_BYPASS_START:
          self.events.add(EventName.ldw)
          CC.latActive = False
          CC.longActive = False
        if self._dp_vag_timebomb_bypass_counter >= DP_VAG_TIMEBOMB_BYPASS_END:
          self._dp_vag_timebomb_bypass_counter = 0

    actuators = CC.actuators
    actuators.longControlState = self.LoC.long_control_state

    if self.sm['lateralPlan'].laneChangeState != LaneChangeState.off:
      CC.leftBlinker = self.sm['lateralPlan'].laneChangeDirection == LaneChangeDirection.left
      CC.rightBlinker = self.sm['lateralPlan'].laneChangeDirection == LaneChangeDirection.right

    if CS.leftBlinker or CS.rightBlinker:
      self.last_blinker_frame = self.sm.frame

    if not CC.latActive:
      self.LaC.reset()
    if not CC.longActive:
      self.LoC.reset(v_pid=CS.vEgo)

    if not self.joystick_mode:
      pid_accel_limits = self.CI.get_pid_accel_limits(self.CP, CS.vEgo, self.v_cruise_helper.v_cruise_kph * CV.KPH_TO_MS)
      t_since_plan = (self.sm.frame - self.sm.rcv_frame['longitudinalPlan']) * DT_CTRL
      actuators.accel = self.LoC.update(CC.longActive, CS, long_plan, pid_accel_limits, t_since_plan)

      if self._adaptive_accel_enabled:
        if lead_one.status and lead_one.dRel > 5.0:  # 【可调】最小跟车距离5米
          distance, lead_speed, ego_speed = lead_one.dRel, lead_one.vLead, CS.vEgo
          speed_diff = ego_speed - lead_speed
          ideal_dist = 1.85 * ego_speed + 4.0  # 【可调】理想距离=时间间隙1.85秒×速度+4米
          deviation = ideal_dist - distance
          if deviation < -3.0 and speed_diff < -0.8:  # 【可调】加速触发条件（距离偏差-3米，速度差-0.8m/s）
            intensity = min(abs(deviation)/20.0, 1.0) * min(abs(speed_diff)/4.0, 1.0) * 0.5  # 【可调】加速强度减半
            distance_bonus = min(distance/60.0, 0.15)  # 【可调】距离奖励因子减半（分母60米，上限0.15）
            final_intensity = min(intensity + distance_bonus, 0.8)
            target_accel = min(pid_accel_limits[1], 1.2) * final_intensity  # 【可调】最大加速度从1.8降至1.2 m/s²
            actuators.accel = max(actuators.accel, target_accel)

      self.desired_curvature, self.desired_curvature_rate = get_lag_adjusted_curvature(self.CP, CS.vEgo,
                                                                                       lat_plan.psis,
                                                                                       lat_plan.curvatures,
                                                                                       lat_plan.curvatureRates)

      lat_tuning = self.CP.lateralTuning.which()
      if lat_tuning == 'torque':
        actuators.steer, actuators.steeringAngleDeg, lac_log = self.LaC.update(CC.latActive, CS, self.VM, lp,
                                                                             self.last_actuators, self.steer_limited, self.desired_curvature,
                                                                             self.desired_curvature_rate, self.sm['liveLocationKalman'], model_data=model_v2)
      else:
        actuators.steer, actuators.steeringAngleDeg, lac_log = self.LaC.update(CC.latActive, CS, self.VM, lp,
                                                                             self.last_actuators, self.steer_limited, self.desired_curvature,
                                                                             self.desired_curvature_rate, self.sm['liveLocationKalman'])
      actuators.curvature = self.desired_curvature
    else:
      lac_log = log.ControlsState.LateralDebugState.new_message()
      if self.sm.rcv_frame['testJoystick'] > 0:
        if CC.longActive:
          actuators.accel = 4.0*clip(self.sm['testJoystick'].axes[0], -1, 1)
        if CC.latActive:
          steer = clip(self.sm['testJoystick'].axes[1], -1, 1)
          actuators.steer, actuators.steeringAngleDeg, actuators.curvature = steer, steer * 45., steer * -0.02
        lac_log.active = self.active
        lac_log.steeringAngleDeg = CS.steeringAngleDeg
        lac_log.output = actuators.steer
        lac_log.saturated = abs(actuators.steer) >= 0.9

    if CS.steeringPressed:
      self.last_steering_pressed_frame = self.sm.frame
    recent_steer_pressed = (self.sm.frame - self.last_steering_pressed_frame)*DT_CTRL < 2.0

    if lac_log.active and not recent_steer_pressed and not self.CP.notCar:
      if self.CP.lateralTuning.which() == 'torque' and not self.joystick_mode:
        undershooting = abs(lac_log.desiredLateralAccel) / abs(1e-3 + lac_log.actualLateralAccel) > 1.2
        turning = abs(lac_log.desiredLateralAccel) > 1.0
        good_speed = CS.vEgo > 5
        max_torque = abs(self.last_actuators.steer) > 0.99
        if undershooting and turning and good_speed and max_torque:
          lac_log.active and self.events.add(EventName.steerSaturated)
      elif lac_log.saturated:
        dpath_points = lat_plan.dPathPoints
        if len(dpath_points):
          if self.CP.steerControlType == car.CarParams.SteerControlType.angle:
            steering_value = actuators.steeringAngleDeg
          else:
            steering_value = actuators.steer
          left_deviation = steering_value > 0 and dpath_points[0] < -0.20
          right_deviation = steering_value < 0 and dpath_points[0] > 0.20
          if left_deviation or right_deviation:
            self.events.add(EventName.steerSaturated)

    for p in ACTUATOR_FIELDS:
      attr = getattr(actuators, p)
      if not isinstance(attr, SupportsFloat):
        continue
      if not math.isfinite(attr):
        cloudlog.error(f"actuators.{p} not finite {actuators.to_dict()}")
        setattr(actuators, p, 0.0)

    return CC, lac_log

  def publish_logs(self, CS, start_time, CC, lac_log):
    """Send actuators and hud commands to the car, send controlsstate and MPC logging"""
    orientation_value = list(self.sm['liveLocationKalman'].calibratedOrientationNED.value)
    if len(orientation_value) > 2:
      CC.orientationNED = orientation_value
    angular_rate_value = list(self.sm['liveLocationKalman'].angularVelocityCalibrated.value)
    if len(angular_rate_value) > 2:
      CC.angularVelocity = angular_rate_value

    CC.cruiseControl.override = self.enabled and not CC.longActive and self.CP.openpilotLongitudinalControl
    CC.cruiseControl.cancel = CS.cruiseState.enabled and (not self.enabled or not self.CP.pcmCruise)
    if self.joystick_mode and self.sm.rcv_frame['testJoystick'] > 0 and self.sm['testJoystick'].buttons[0]:
      CC.cruiseControl.cancel = True

    speeds = self.sm['longitudinalPlan'].speeds
    accels = self.sm['longitudinalPlan'].accels
    
    resume_raw = self.enabled and CS.cruiseState.standstill and (len(speeds) > 0 and speeds[-1] > 0.1)
    
    if resume_raw:
      self.resume_filter_count += 1
    else:
      self.resume_filter_count = 0
    
    lead_one = self.sm['radarState'].leadOne
    resume_threshold = 6 if lead_one.status else 20
    CC.cruiseControl.resume = self.resume_filter_count >= resume_threshold

    hudControl = CC.hudControl
    hudControl.setSpeed = float(self.v_cruise_helper.v_cruise_cluster_kph * CV.KPH_TO_MS)
    hudControl.speedVisible = self.enabled
    hudControl.lanesVisible = self.enabled
    hudControl.leadVisible = self.sm['longitudinalPlan'].hasLead

    hudControl.rightLaneVisible = True
    hudControl.leftLaneVisible = True

    recent_blinker = (self.sm.frame - self.last_blinker_frame) * DT_CTRL < 5.0
    ldw_allowed = self.is_ldw_enabled and CS.vEgo > LDW_MIN_SPEED and not recent_blinker \
                  and not CC.latActive and self.sm['liveCalibration'].calStatus == log.LiveCalibrationData.Status.calibrated

    model_v2 = self.sm['modelV2']
    desire_prediction = model_v2.meta.desirePrediction
    if len(desire_prediction) and ldw_allowed:
      right_lane_visible = model_v2.laneLineProbs[2] > 0.5
      left_lane_visible = model_v2.laneLineProbs[1] > 0.5
      l_lane_change_prob = desire_prediction[Desire.laneChangeLeft]
      r_lane_change_prob = desire_prediction[Desire.laneChangeRight]

      lane_lines = model_v2.laneLines
      l_lane_close = left_lane_visible and (lane_lines[1].y[0] > -(1.08 + CAMERA_OFFSET))
      r_lane_close = right_lane_visible and (lane_lines[2].y[0] < (1.08 - CAMERA_OFFSET))

      hudControl.leftLaneDepart = bool(l_lane_change_prob > LANE_DEPARTURE_THRESHOLD and l_lane_close)
      hudControl.rightLaneDepart = bool(r_lane_change_prob > LANE_DEPARTURE_THRESHOLD and r_lane_close)

    if hudControl.rightLaneDepart or hudControl.leftLaneDepart:
      self.events.add(EventName.ldw)

    clear_event_types = set()
    if ET.WARNING not in self.current_alert_types:
      clear_event_types.add(ET.WARNING)
    if self.enabled:
      clear_event_types.add(ET.NO_ENTRY)

    alerts = self.events.create_alerts(self.current_alert_types, [self.CP, CS, self.sm, self.is_metric, self.soft_disable_timer])
    self.AM.add_many(self.sm.frame, alerts)
    current_alert = self.AM.process_alerts(self.sm.frame, clear_event_types)
    if current_alert:
      hudControl.visualAlert = current_alert.visual_alert

    if not self.read_only and self.initialized:
      now_nanos = self.can_log_mono_time if REPLAY else int(time.monotonic() * 1e9)
      self.last_actuators, can_sends = self.CI.apply(CC, now_nanos)
      self.pm.send('sendcan', can_list_to_can_capnp(can_sends, msgtype='sendcan', valid=CS.canValid))
      CC.actuatorsOutput = self.last_actuators
      if self.CP.steerControlType == car.CarParams.SteerControlType.angle:
        self.steer_limited = abs(CC.actuators.steeringAngleDeg - CC.actuatorsOutput.steeringAngleDeg) > \
                             STEER_ANGLE_SATURATION_THRESHOLD
      else:
        self.steer_limited = abs(CC.actuators.steer - CC.actuatorsOutput.steer) > 1e-2

    force_decel = (not NO_IR_CTRL and self.sm['driverMonitoringState'].awarenessStatus < 0.) or (self.state == State.softDisabling)

    lp = self.sm['liveParameters']
    steer_angle_without_offset = math.radians(CS.steeringAngleDeg - lp.angleOffsetDeg)
    curvature = -self.VM.calc_curvature(steer_angle_without_offset, CS.vEgo, lp.roll)

    dat = messaging.new_message('controlsState')
    dat.valid = CS.canValid
    controlsState = dat.controlsState
    if current_alert:
      controlsState.alertText1 = current_alert.alert_text_1
      controlsState.alertText2 = current_alert.alert_text_2
      controlsState.alertSize = current_alert.alert_size
      controlsState.alertStatus = current_alert.alert_status
      controlsState.alertBlinkingRate = current_alert.alert_rate
      controlsState.alertType = current_alert.alert_type
      controlsState.alertSound = current_alert.audible_alert

    controlsState.longitudinalPlanMonoTime = self.sm.logMonoTime['longitudinalPlan']
    controlsState.lateralPlanMonoTime = self.sm.logMonoTime['lateralPlan']
    controlsState.enabled = self.enabled
    controlsState.active = self.active
    controlsState.curvature = curvature
    controlsState.desiredCurvature = self.desired_curvature
    controlsState.state = self.state
    controlsState.engageable = not self.events.contains(ET.NO_ENTRY)
    controlsState.longControlState = self.LoC.long_control_state
    controlsState.vPid = float(self.LoC.v_pid)
    controlsState.vCruise = float(self.v_cruise_helper.v_cruise_kph)
    controlsState.vCruiseCluster = float(self.v_cruise_helper.v_cruise_cluster_kph)
    controlsState.upAccelCmd = float(self.LoC.pid.p)
    controlsState.uiAccelCmd = float(self.LoC.pid.i)
    controlsState.ufAccelCmd = float(self.LoC.pid.f)
    a_target = get_accel_from_plan(self.CP, speeds, accels)
    controlsState.aTarget = a_target
    controlsState.cumLagMs = -self.rk.remaining * 1000.
    controlsState.startMonoTime = int(start_time * 1e9)
    controlsState.forceDecel = bool(force_decel)
    controlsState.canErrorCounter = self.can_rcv_cum_timeout_counter
    controlsState.experimentalMode = self.experimental_mode

    lat_tuning = self.CP.lateralTuning.which()
    if self.joystick_mode:
      controlsState.lateralControlState.debugState = lac_log
    elif self.CP.steerControlType == car.CarParams.SteerControlType.angle:
      controlsState.lateralControlState.angleState = lac_log
    elif lat_tuning == 'pid':
      controlsState.lateralControlState.pidState = lac_log
    elif lat_tuning == 'torque':
      controlsState.lateralControlState.torqueState = lac_log
    elif lat_tuning == 'indi':
      controlsState.lateralControlState.indiState = lac_log
    elif lat_tuning == 'lqr':
      controlsState.lateralControlState.lqrState = lac_log

    self.pm.send('controlsState', dat)

    dat = messaging.new_message('controlsStateExt')
    dat.valid = CS.canValid
    controlsStateExt = dat.controlsStateExt
    controlsStateExt.alkaActive = self._dp_alka_active
    controlsStateExt.alkaEnabled = self._dp_alka
    self.pm.send('controlsStateExt', dat)

    car_events = self.events.to_msg()
    cs_send = messaging.new_message('carState')
    cs_send.valid = CS.canValid
    cs_send.carState = CS
    cs_send.carState.events = car_events
    self.pm.send('carState', cs_send)

    if (self.sm.frame % int(1. / DT_CTRL) == 0) or (self.events.names != self.events_prev):
      ce_send = messaging.new_message('carEvents', len(self.events))
      ce_send.carEvents = car_events
      self.pm.send('carEvents', ce_send)
    self.events_prev = self.events.names.copy()

    if (self.sm.frame % int(50. / DT_CTRL) == 0):
      cp_send = messaging.new_message('carParams')
      cp_send.carParams = self.CP
      self.pm.send('carParams', cp_send)

    cc_send = messaging.new_message('carControl')
    cc_send.valid = CS.canValid
    cc_send.carControl = CC
    self.pm.send('carControl', cc_send)

    self.CC = CC

  def step(self):
    start_time = time.monotonic()
    self.prof.checkpoint("Ratekeeper", ignore=True)

    self.is_metric = self.params.get_bool("IsMetric")
    self.experimental_mode = self.params.get_bool("ExperimentalMode") and self.CP.openpilotLongitudinalControl
    if self.CP.radarUnavailable and self.dp_0813:
      self.experimental_mode = False
    
    if self.force_experimental_mode:
      self.experimental_mode = True

    CS = self.data_sample()
    cloudlog.timestamp("Data sampled")
    self.prof.checkpoint("Sample")

    self.update_events(CS)
    cloudlog.timestamp("Events updated")

    if not self.read_only and self.initialized:
      self.state_transition(CS)
      self.prof.checkpoint("State transition")

    CC, lac_log = self.state_control(CS)
    self.prof.checkpoint("State Control")

    self.publish_logs(CS, start_time, CC, lac_log)
    self.prof.checkpoint("Sent")

    self.CS_prev = CS

  def controlsd_thread(self):
    while True:
      self.step()
      self.rk.monitor_time()
      self.prof.display()

def main(sm=None, pm=None, logcan=None):
  controls = Controls(sm, pm, logcan)
  controls.controlsd_thread()

if __name__ == "__main__":
  main()