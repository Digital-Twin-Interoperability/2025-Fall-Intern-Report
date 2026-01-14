import os
import json
import math
import carb
import tempfile
import omni.usd
import omni.physx
from pathlib import Path
from datetime import datetime
from omni.kit.scripting import BehaviorScript
from pxr import Gf, UsdGeom, Usd, UsdPhysics, PhysxSchema, PhysicsSchemaTools
from omni.physx import get_physx_simulation_interface

#============= USER CONFIG =============
CONFIG = {
    # USD prim paths 
    "prim_paths": {
        # Chassis Prim of Rover script is attached to
        "self_chassis": "/World/Omni_Cadre/CADRE_Demo/Chassis",
        
        # Chassis Prim of the other rover that is in the world
        "other_rover_chassis": "/World/Unity_Viper/Xform/Chassis",
        
        # Wheel Joint Prims
        "wheel_joints": [
            "/World/Omni_Cadre/CADRE_Demo/FR/RevoluteJoint",
            "/World/Omni_Cadre/CADRE_Demo/FL/RevoluteJoint",
            "/World/Omni_Cadre/CADRE_Demo/BR/RevoluteJoint",
            "/World/Omni_Cadre/CADRE_Demo/BL/RevoluteJoint",
        ],
    },
    
    # Joint Drive Configuration
    "drive": {
        "velocity_attr": "drive:angular:physics:targetVelocity",
        "default_velocity": 400.0,      # used in dynamic mode
        "stop_velocity": 0.0,           # used in kinematic mode
    },
    
    # Producer/Consider JSON
    "io": {
        # Producer output JSON
        # use_tempdir=True: file is written to OS temp dir with filename below
        # use_tempdir=False: set output_path to an absolute path
        "producer_output": {
            "use_tempdir": True,
            "filename": "producerOmniUpdate.json",
            "output_path": "",          # only used if use_tempdir=False
        },
        
        # Consumer input JSON
        "consumer_input_path": r"D:/School/Research/Current Research/NASA-JPL Digital Twin Interoperability/collision_ideas/code/consumerOmniUpdate.json",
    },
    
    # Expected schema of the consumer JSON
    "consumer_schema": {
        "position_list_key": "position",
        "rotation_list_key": "rotation",
        "name_key": "name",
        "value_key": "value",
        
        # Field names expected in the lists
        "position_fields": {
            "x": "xCoordinate",
            "y": "zCoordinate",
            "z": "yCoordinate",
        },
        
        "rotation_fields": {
            "w": "w",
            "x": "rx",
            "y": "ry",
            "z": "rz",
        },
    },
    
    # Collision Detection Settings
    "collision": {
        # Substrings used to detect the rovers in PhysX contact paths
        "self_match_substring": "Omni_Cadre/CADRE_Demo",
        "other_match_substring": "Unity_Viper",
        
        # PhysX contact threshold
        "contact_threshold": 0.0,
        
        # Whether to apply contact reporting API to the other rover prim
        "apply_contact_api_to_other": True
    },
    
    # Behavior Toggles
    "behavior": {
        "hosting_default": False,       # Default value for self.hosting_default 
        "start_tracking_default": True, # Default value for self.track_rover
        "switch_to_consumer_on_collision": True,
    },
    
    "colliders": {
        # If provided, used directly
        # If not provided, colliders are discovered
        "explicit_paths": [],
        
        # Root prim to scan for CollisionAPI
        "discover_under_prim": "",
        
        # Optional
        "mesh_only": False,
    },
    
    "export": {
        "include_velocity": True, 
        "include_colliders": True,
        "include_collider_approximation": True,
        "include_last_collision": True,
    }
}

#============= BehaviorScript =============
previous_state = {}

def _resolve_producer_output_path(cfg: dict) -> Path:
    out_cfg = cfg["io"]["producer_output"]
    if out_cfg.get("use_tempdir", True):
        out_path = Path(tempfile.gettempdir()) / out_cfg.get("filename", "producerOmniUpdate.json")
    else:
        raw = out_cfg.get("output_path", "").strip()
        if not raw:
            raise ValueError("CONFIG['io']['producer_output']['output_path'] must be set when use_tempdir=False")
        out_path = Path(raw)
    
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path

def _safe_set_attr(prim, attr_name: str, value) -> bool:
    if not prim or not prim.IsValid():
        return False
    
    attr = prim.GetAttribute(attr_name)
    
    if not attr or not attr.IsValid():
        return False
    
    try:
        attr.Set(value)
        return True
    except Exception:
        return False

def _gfvec3_to_list(v):
    if v is None:
        return None
    
    try:
        return [float(v[0]), float(v[1]), float(v[2])]
    except Exception:
        return None

def _get_mesh_approximation_token(prim):
    try:
        mesh_api = UsdPhysics.MeshCollisionAPI(prim)
        if not mesh_api:
            return None
        
        attr = mesh_api.GetApproximationAttr()
        
        if attr and attr.IsValid():
            val = attr.Get()
            return str(val) if val is not None else None
    
    except Exception:
        pass
    
    return None

def _discover_collider(stage, root_prim, mesh_only: bool=False):
    out = []
    if not root_prim or not root_prim.IsValid():
        return out 
    
    for prim in Usd.PrimRange(root_prim):
        if not prim or not prim.IsValid():
            continue
        
        try:
            col_api = UsdPhysics.CollisionAPI(prim)
        except Exception:
            col_api = None
        
        if not col_api:
            continue
        
        if mesh_only:
            if _get_mesh_approximation_token(prim) is None:
                continue
        
        out.append(prim)
    
    return out

class Rover(BehaviorScript):
    def on_init(self):
        # Defaults from config 
        self.hosting = bool(CONFIG["behavior"].get("hosting_default", False))
        self.track_rover = bool(CONFIG["behavior"].get("start_tracking_default", True))
        
        # Get Stage
        stage = omni.usd.get_context().get_stage()
        
        # Get Prim Paths
        self.chassis_path = CONFIG["prim_paths"]["self_chassis"]
        self._other_rover_path = CONFIG["prim_paths"]["other_rover_chassis"]
        
        # Chassis Prim and Transform Ops 
        self.chassis_prim = stage.GetPrimAtPath(self.chassis_path)
        if not self.chassis_prim.IsValid():
            print(f"[ERROR] Prim not found at path: {self.chassis_path}")
            self.chassis_rigid = None
            return
        
        self.chassis_xformable = UsdGeom.Xformable(self.chassis_prim)
        
        self.translate_op = None
        self.orient_op = None
        for op in self.chassis_xformable.GetOrderedXformOps():
            if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                self.translate_op = op
            elif op.GetOpType() == UsdGeom.XformOp.TypeOrient:
                self.orient_op = op
                
        if self.translate_op is None:
            self.translate_op = self.chassis_xformable.AddTranslateOp(UsdGeom.XformOp.PrescisionDouble)
        
        if self.orient_op is None:
            self.orient_op = self.chassis_xformable.AddOrientOp(UsdGeom.XformOp.PrecisionDouble)
        
        # RigidBody 
        self.chassis_rigid = UsdPhysics.RigidBodyAPI(self.chassis_prim)
        if not self.chassis_rigid:
            print(f"[ERROR] Prim is not a PhysX RigidBody: {self.chassis_path}")
            self.chassis_rigid = UsdPhysics.RigidBodyAPI.Apply(self.chassis_prim)
            return
        
        # Joints 
        joint_paths = CONFIG["prim_paths"].get("wheels_joints", [])
        self.joints = [stage.GetPrimAtPath(p) for p in joint_paths]
        
        # Drive Attribute
        self.velocity_attr = CONFIG["drive"]["velocity_attr"]
        
        # Validate Joints
        for jp, j in zip(joint_paths, self.joints):
            if not j or not j.IsValid():
                carb.log_error(f"[ERROR] Joint prim invalid: {jp}")
                continue
            
            print(f"[INFO] Joint prim: {j.GetPath()}")
            attr = j.GetAttribute(self.velocity_attr)
            
            if not attr or not attr.IsValid():
                carb.log_error(f"[ERROR] Attribute '{self.velocity_attr}' NOT found on {j.GetPath()}")
            else:
                print(f"[INFO] Attr {self.velocity_attr} typeName:", attr.GetTypeName())
        
        # Ensure system stars dynamic
        self.set_dynamic()
        
        # JSON paths
        self.output_path = _resolve_producer_output_path(CONFIG)
        self.file_path = CONFIG["io"]["consumer_input_path"]
        
        # Save initial state for restoration
        if self.translate_op:
            pos_val = self.translate_op.Get()
            if pos_val is None:
                pos_val = Gf.Vec3d(0.0, 0.0, 0.0)
                self.translate_op.Set(pos_val)
            self.initial_local_pos = pos_val
        else:
            self.initial_local_pos = Gf.Vec3d(0.0, 0.0, 0.0)
        
        if self.orient_op:
            rot_val = self.orient_op.Get()
            if rot_val is None:
                rot_val = Gf.Quatd(1.0, 0.0, 0.0, 0.0)
                self.orient_op.Set(rot_val)
            self.initial_local_rot = rot_val
        else:
            self.initial_local_rot = Gf.Quatf(1.0, 0.0, 0.0, 0.0)
        
        kin_attr = self.chassis_rigid.GetKinematicEnabledAttr()
        self.initial_kinematic = kin_attr.Get() if kin_attr and kin_attr.HasAuthoredValue() else False
        
        self.initial_wheel_velocities = {}
        for j in self.joints:
            if not j or not j.IsValid():
                continue
            attr = j.GetAttribute(self.velocity_attr)
            if attr and attr.IsValid():
                self.initial_wheel_velocities[j.GetPath()] = attr.Get()
            else:
                self.initial_wheel_velocities[j.GetPath()] = 0.0
        
        # Contact Reporting
        threshold = float(CONFIG["collision"].get("contact_threshold", 0.0))
        contact_api = PhysxSchema.PhysxContactReportAPI.Apply(self.chassis_prim)
        contact_api.CreateThresholdAttr().Set(threshold)
        
        self._sim_iface = get_physx_simulation_interface()
        self._contact_sub_id = self._sim_iface.subscribe_contact_report_events(self._on_contact_report)
        
        if CONFIG["collision"].get("apply_contact_api_to_other", True):
            other_prim = stage.GetPrimAtPath(self._other_rover_path)
            if other_prim and other_prim.IsValid():
                other_contact = PhysxSchema.PhysxContactReportAPI.Apply(other_prim)
                other_contact.CreateThresholdAttr().Set(threshold)
                print("[INFO] Other rover contact API enabled.")
            else:
                print(f"[WARNING] Other rover prim not valid at path: {self._other_rover_path}")
        
        self._collision_flag = False
        
        # Collider discovery and cache 
        self._collider_prims = []
        self._last_collision = None
        
        if CONFIG.get("export", {}).get("include_colliders", True):
            explicit = CONFIG.get("colliders", {}).get("explicit_paths", []) or []
        
            if explicit:
                self._collider_prims = [stage.GetPrimAtPath(p) for p in explicit if stage.GetPrimAtPath(p).IsValid()]
            else:
                discover_root_path = (CONFIG.get("colliders", {}).get("discover_under_prim") or "").strip()
                
                if discover_root_path:
                    discover_root = stage.GetPrimAtPath(discover_root_path)
                else:
                    discover_root = self.chassis_prim
                
                self._collider_prims = _discover_collider(
                    stage, discover_root, 
                    mesh_only=bool(CONFIG.get("colliders", {}).get("mesh_only", False)),
                )
        
        print(f"[INFO] Cached {len(self._collider_prims)} collider prim(s).")
        print("[INFO] Subscribed to contact report events")
        print(f"[INFO] Producer JSON file saved to {self.output_path}")
        print(f"[INFO] Consumer JSON file to read: {self.file_path}")
        print(f"[INFO] Tracker initialized for prim: {self.chassis_path}")
        print("[INFO] Initialization DONE")
    
    def on_update(self, current_time: float, delta_time: float):
        switch_on_collision = bool(CONFIG["behavior"].get("switch_to_consumer_on_collision", True))
        
        if self.hosting:
            self.set_dynamic()
            self.write_json()
        else:
            if self.track_rover:
                self.set_dynamic()
                self.write_json()
                
                if switch_on_collision and self._collision_flag:
                    print("[INFO] Collision detected; switching to consumer mode (kinematic + read).")
                    self.set_kinematic()
                    self.track_rover = False
            else:
                self.set_kinematic()
                self.read_json()
    
    def on_stop(self):
        self.set_dynamic()
        self.track_rover = True
        self._collision_flag = False
        
        print("[INFO] on_stop() - restoring rover to initial state")
        
        if self.chassis_rigid:
            kin_attr = self.chassis_rigid.GetKinematcEnabledAttr()
            if not kin_attr:
                kin_attr = self.chassis_rigid.CreateKinematicEnabledAttr()
            kin_attr.Set(self.initial_kinematic)
        
        for j in self.joints:
            if not j or not j.IsValid():
                continue
            attr = j.GetAttribute(self.velocity_attr)
            if attr and attr.IsValid():
                v0 = self.initial_wheel_velocities.get(j.GetPath(), 0.0)
                attr.Set(v0)
        
        try:
            if self.translate_op:
                self.translate_op.Set(self.initial_local_pos)
            if self.orient_op:
                self.orient_op.Set(self.initial_local_pos)
            print("[INFO] Rover transform reset to initil pose.")
        except Exception as e:
            print(f"[ERROR] Failed to restore transform in on_stop: {e}")
    
    def on_shutdown(self):
        self.set_dynamic()
        self.track_rover = True
        self._collision_flag = False
        print("[INFO] Shutting down script.")
    
    def on_destroy(self):
        self._collision_flag = False
        self.set_dynamic()
        self.track_rover = True
    
    def write_json(self):
        try:
            current_data = self.get_rigid_body_physics()
            
            global previous_state
            if current_data != previous_state:
                with open(self.output_path, "w") as f:
                    json.dump(current_data, f, indent=4)
                previous_state = current_data
                print(f"[INFO] Wrote new transform data to {self.output_path}")
        except Exception as e:
            print(f"[ERROR] Exception during update: {e}")
    
    def quarternion_to_unity_euler(self, w, x, y, z):
        x_unity = -x 
        y_unity = y 
        z_unity = -z 
        w_unity = w 
        
        sinr_cosp = 2 * (w_unity * x_unity + y_unity * z_unity)
        cosr_cosp = 1 - 2 * (x_unity * x_unity + y_unity * y_unity)
        roll = math.atan2(sinr_cosp, cosr_cosp)
        
        sinp = 2 * (w_unity * y_unity - z_unity * x_unity)
        if abs(sinp) >= 1:
            pitch = math.copysign(math.pi / 2, sinp)
        else:
            pitch = math.asin(sinp)
        
        siny_cosp = 2 * (w_unity * y_unity - z_unity * x_unity)
        cosy_cosp = 1 - 2 * (y_unity * y_unity + z_unity * z_unity)
        yaw = math.atan2(siny_cosp, cosy_cosp)
        
        return [math.degrees(roll), math.degrees(pitch), math.degrees(yaw)]
    
    def get_rigid_body_physics(self):
        matrix = omni.usd.get_world_transform_matrix(self.chassis_prim)
        translate = matrix.ExtractTranslation()
        rotation = matrix.ExtractRotation().GetQuaternion()
        
        w = rotation.GetReal()
        x, y, z = rotation.GetImaginary()
        
        position = [float(translate[0]), float(translate[1]), float(translate[2])]
        rotation_q = self.quarternion_to_unity_euler(w, x, y, z)
        
        mass_api = UsdPhysics.MassAPI(self.chassis_prim)
        
        mass = None
        com = None
        inertia_type = None
        inertia_values = None 
        
        if mass_api:
            mass = mass_api.GetMassAttr().Get()
            com = mass_api.GetCenterOfMassAttr().Get()
            
            diag = mass_api.GetDiagonalInertiaAttr().Get()
            if diag is not None:
                inertia_type = "diagonal"
                inertia_values = [float(diag[0]), float(diag[1]), float(diag[2])]
            else:
                full = mass_api.GetInertiaAttr().Get()
                if full is not None:
                    inertia_type = "full_matrix"
                    inertia_values = [
                        [float(full[0]), float(full[1]), float(full[2])],
                        [float(full[3]), float(full[4]), float(full[5])],
                        [float(full[6]), float(full[7]), float(full[8])],

                    ]
        
        lin_vel = None
        ang_vel = None
        if CONFIG.get("export", {}).get("include_velocity", True) and self.chassis_rigid:
            
            try:
                v_attr = self.chassis_rigid.GetVelocityAttr()
                w_attr = self.chassis_rigid.GetAngularVelocityAttr()
                lin_vel = _gfvec3_to_list(v_attr.Get() if v_attr and v_attr.IsValid() else None)
                ang_vel = _gfvec3_to_list(w_attr.Get() if w_attr and w_attr.IsValid() else None)
            except Exception:
                pass
        
        colliders_payload = None
        if CONFIG.get("export", {}).get("include_colliders", True):
            colliders_payload = []
            for prim in (self._collider_prims or []):
                if not prim or not prim.IsValid():
                    continue
                
                approx = None
                if CONFIG.get("export", {}).get("include_collider_approximation", True):
                    approx = _get_mesh_approximation_token(prim)
                
                colliders_payload.append({
                    "prim_path": str(prim.GetPath()),
                    "type_name": prim.GetTypeName(),
                    "mesh_approximation": approx,
                })
        
        lass_collision_payload = None
        if CONFIG.get("export", {}).get("include_last_collision", True):
            last_collision_payload = self._last_collision
        
        data = {
            "position": position,
            "rotation": rotation_q,
            "mass": float(mass) if mass is not None else None,
            "inertia": {"type": inertia_type, "values": inertia_values},
            "center_of_mass": (
                [float(com[0]), float(com[1]), float(com[2])] if com is not None else None
            ),
            "linear_velocity": lin_vel,
            "angular_velocity": ang_vel,
            "colliders": colliders_payload,
            "last_collision": last_collision_payload,
            
            "hosting": self.hosting,
            "modifiedDate": datetime.now().isoformat(),
        }
        return data
    
    def set_kinematic(self):
        if not self.chassis_rigid:
            return
        
        attr = self.chassis_rigid.GetKinematicEnabledAttr()
        
        if not attr:
            attr = self.chassis_rigid.CreateKinematicEnabledAttr()
        
        attr.Set(True)
        self.set_wheel_velocities(velocity=float(CONFIG["drive"].get("stop_velocity", 0.0)))
        print("[INFO] Rover set to KINEMATIC mode.")
    
    def set_dynamic(self):
        if not self.chassis_rigid:
            return
        
        attr = self.chassis_rigid.GetKinematicEnabledAttr()
        
        if not attr:
            attr = self.chassis_rigid.CreateKinematicEnabledAttr()
        
        attr.Set(False)
        self.set_wheel_velocities(velocity=float(CONFIG["drive"].get("default_velocity", 400.0)))
        print("[INFO] Rover set to DYNAMIC (physics) mode.")
    
    def set_wheel_velocities(self, velocity: float):
        for joint in self.joints:
            if not joint or not joint.IsValid():
                continue
            
            ok = _safe_set_attr(joint, self.velocity_attr, float(velocity))
            
            if not ok:
                carb.log_warn(f"[WARN] Could not set '{self.velocity_attr}' on {joint.GetPath()}")
    
    def read_json(self):
        try:
            if not os.path.exists(self.file_path):
                print(f"[ERROR] File not found: {self.file_path}")
                return
            
            with open(self.file_path, "r") as file:
                data = json.load(file)
            
            schema = CONFIG["consumer_schema"]
            pos_list = data.get(schema["position_list_key"], [])
            rot_list = data.get(schema["rotation_list_key"], [])
            
            name_key = schema["name_key"]
            value_key = schema["value_key"]
            
            pos_map = {prop.get(name_key): prop.get(value_key) for prop in pos_list if isinstance(prop, dict)}
            rot_map = {prop.get(name_key): prop.get(value_key) for prop in rot_list if isinstance(prop, dict)}
            
            # Position Mapping
            pf = schema["posiiton_fields"]
            x = float(pos_map.get(pf["x"], 0.0))
            y = float(pos_map.get(pf["y"], 0.0))
            z = float(pos_map.get(pf["z"], 0.0))

            # Rotation mapping
            rf = schema["rotation_fields"]
            w = float(rot_map.get(rf["w"], 1.0))
            rx = float(rot_map.get(rf["x"], 0.0))
            ry = float(rot_map.get(rf["y"], 0.0))
            rz = float(rot_map.get(rf["z"], 0.0))

            desired_position = Gf.Vec3d(x, y, z)
            desired_rotation = Gf.Quatd(w, rx, ry, rz)

            parent_prim = self.chassis_prim.GetParent()
            parent_world_transform = UsdGeom.Xformable(parent_prim).ComputeLocalToWorldTransform(Usd.TimeCode.Default())
            if not parent_world_transform:
                print(f"[WARN] Parent prim at {parent_prim.GetPath()} has no transform. Using identity.")
                parent_world_transform = Gf.Matrix4d(1.0)

            parent_world_inverse = parent_world_transform.GetInverse()
            local_position = parent_world_inverse.TransformAffine(desired_position)

            self.translate_op.Set(local_position)
            self.orient_op.Set(desired_rotation)

        except Exception as e:
            print(f"[ERROR] Error reading or updating prim: {e}")
    
    def _on_contact_report(self, contact_headers, contact_data):
        self_match = str(CONFIG["collision"].get("self_match_substring", ""))
        other_match = str(CONFIG["collision"].get("other_match_substring", ""))

        for header in contact_headers:
            actor0_path = str(PhysicsSchemaTools.intToSdfPath(header.actor0))
            actor1_path = str(PhysicsSchemaTools.intToSdfPath(header.actor1))
            collider0_path = str(PhysicsSchemaTools.intToSdfPath(header.collider0))
            collider1_path = str(PhysicsSchemaTools.intToSdfPath(header.collider1))

            involves_self = (
                (self_match and self_match in actor0_path)
                or (self_match and self_match in actor1_path)
                or (self_match and self_match in collider0_path)
                or (self_match and self_match in collider1_path)
            )
            involves_other = (
                (other_match and other_match in actor0_path)
                or (other_match and other_match in actor1_path)
                or (other_match and other_match in collider0_path)
                or (other_match and other_match in collider1_path)
            )

            if involves_self and involves_other:
                self._collision_flag = True
                self._last_collision = {
                    "actor0": actor0_path,
                    "actor1": actor1_path,
                    "collider0": collider0_path,
                    "collider1": collider1_path,
                    "timestamp": datetime.now().isoformat(),
                }
                print("[INFO] Collision detected between self rover and other rover.")
                return

    def _is_collision_between_rovers(self, path0, path1):
        this = self.chassis_path
        other = self._other_rover_path
        return ((path0.startswith(this) and path1.startswith(other)) or (path1.startswith(this) and path0.startswith(other)))