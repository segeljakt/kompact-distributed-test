#![feature(fn_traits)]
#![feature(unboxed_closures)]

use kompact::prelude::*;
use kompact::serde_serialisers::Serde;
use serde::{Deserialize, Serialize};
use std::mem;
use std::net::*;
use std::sync::Arc;
use std::time::Duration;

#[derive(ComponentDefinition)]
struct Driver {
    ctx: ComponentContext<Self>,
    managers: Vec<ActorPath>,
}

#[derive(ComponentDefinition)]
struct Manager {
    ctx: ComponentContext<Self>,
    driver: ActorPath,
    safe_mode: bool,
    workers: Vec<Arc<Component<Worker>>>,
    iport: RequiredPort<WorkerPort>,
    oport: ProvidedPort<WorkerPort>,
}

#[derive(ComponentDefinition, Actor)]
struct Worker {
    ctx: ComponentContext<Self>,
    iport: RequiredPort<WorkerPort>,
    oport: ProvidedPort<WorkerPort>,
    fun: fn(i32) -> i32,
}

impl Worker {
    fn new(fun: fn(i32) -> i32) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            iport: RequiredPort::uninitialised(),
            oport: ProvidedPort::uninitialised(),
            fun,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
enum DriverMessage {
    RegisterUnsafely,
    RegisterSafely,
}

impl SerialisationId for DriverMessage {
    const SER_ID: SerId = 1010;
}

#[derive(Debug, Deserialize, Serialize)]
enum ManagerMessage {
    Input(i32),
    CreateWorkerUnsafely(usize),
    CreateWorkerSafely(PLT),
    //     CreateWorkerDynamically(Box<dyn Udf>),
    CreateWorkerDynamically(Box<dyn Spawn>),
}

// #[derive(Serialize, Deserialize)]
// struct Spawn(Box<dyn Fn(KompactSystem) + Send + Sync>);
//
// impl std::fmt::Debug for Spawn {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         Ok(())
//     }
// }

impl PLT {
    fn get(self) -> *const () {
        match self {
            PLT::UDF0 => udf0 as *const (),
            PLT::UDF1 => udf1 as *const (),
        }
    }
}

impl SerialisationId for ManagerMessage {
    const SER_ID: SerId = 1234;
}

#[derive(Debug)]
struct SpawnMap {
    //     fun: fn(i32) -> i32,
}

// impl FnOnce<(KompactSystem,)> for SpawnMap {
//
//     extern fn call_once(self, (sys,): (KompactSystem,)) -> Self::Output {
//         sys.create(Worker::new)
//     }
// }

#[typetag::serde(tag = "spawn")]
trait Spawn: FnOnce(KompactSystem) + Send + Sync + std::fmt::Debug {}

// #[typetag::serde(name = "spawn")]
// impl Spawn for SpawnMap {}

#[typetag::serde(tag = "udf")]
trait Udf: Send + Sync + std::fmt::Debug {
    fn udf(self, x: i32) -> i32;
}

#[derive(Debug, Serialize, Deserialize)]
struct Udf0;

#[derive(Debug, Serialize, Deserialize)]
struct Udf1;

#[typetag::serde(name = "udf")]
impl Udf for Udf0 {
    fn udf(self, x: i32) -> i32 {
        udf0(x)
    }
}

#[typetag::serde(name = "udf")]
impl Udf for Udf1 {
    fn udf(self, x: i32) -> i32 {
        udf1(x)
    }
}

trait MyTrait: serde_traitobject::Serialize + serde_traitobject::Deserialize {
    fn my_method(&self);
}

#[derive(Serialize, Deserialize)]
struct Message {
    #[serde(with = "serde_traitobject")]
    message: Box<dyn MyTrait>,
}

impl NetworkActor for Driver {
    type Message = DriverMessage;
    type Deserialiser = Serde;

    fn receive(&mut self, manager: Option<ActorPath>, msg: Self::Message) -> Handled {
        let manager = manager.expect("Expected Net Message");
        match msg {
            DriverMessage::RegisterUnsafely => {
                info!(self.log(), "Registering Manager Unsafely {}", manager);
                info!(self.log(), "Telling manager to create workers {}", manager);
                for _ in 0..5 {
                    let bytes = unsafe { mem::transmute::<fn(i32) -> i32, usize>(udf0) };
                    manager.tell((ManagerMessage::CreateWorkerUnsafely(bytes), Serde), self);
                }
            }
            DriverMessage::RegisterSafely => {
                info!(self.log(), "Registering Manager Safely {}", manager);
                info!(self.log(), "Telling manager to create workers {}", manager);
                for _ in 0..5 {
                    let plt = PLT::UDF0;
                    manager.tell((ManagerMessage::CreateWorkerSafely(plt), Serde), self);
                }
            }
        }
        self.managers.push(manager);
        Handled::Ok
    }
}

impl Manager {
    fn create_worker(&mut self, predicate: fn(i32) -> i32) {
        let worker = self.ctx.system().create(move || Worker::new(predicate));
        let oport = &mut self.oport;
        let iport = &mut self.iport;
        if self.workers.is_empty() {
            worker.on_definition(|worker| {
                biconnect_ports(oport, &mut worker.iport);
                biconnect_ports(&mut worker.oport, iport);
            });
        } else {
            let last = self.workers.last().unwrap();
            worker.on_definition(|worker| {
                last.on_definition(|last| biconnect_ports(&mut last.oport, &mut worker.iport));
                biconnect_ports(&mut worker.oport, iport)
            })
        }
        self.ctx.system().start(&worker);
        self.workers.push(worker);
    }
}

impl NetworkActor for Manager {
    type Message = ManagerMessage;
    type Deserialiser = Serde;

    fn receive(&mut self, _: Option<ActorPath>, msg: Self::Message) -> Handled {
        match msg {
            ManagerMessage::CreateWorkerUnsafely(bytes) => {
                info!(self.log(), "Creating worker unsafely");
                let udf = unsafe { mem::transmute::<usize, fn(i32) -> i32>(bytes) };
                self.create_worker(udf);
            }
            ManagerMessage::CreateWorkerSafely(plt) => {
                info!(self.log(), "Creating worker safely");
                let ptr = plt.get();
                let udf = unsafe { mem::transmute::<*const (), fn(i32) -> i32>(ptr) };
                self.create_worker(udf);
            }
            ManagerMessage::Input(data) => {
                info!(self.log(), "Received input data {}", data);
                self.oport.trigger(data)
            }
            ManagerMessage::CreateWorkerDynamically(_udf) => {
                //                 self.create_worker(Udf::udf(udf));
            }
        }
        Handled::Ok
    }
}

struct WorkerPort;

impl Port for WorkerPort {
    type Indication = i32;
    type Request = Never;
}

kompact::prelude::ignore_requests!(WorkerPort, Worker);

impl Require<WorkerPort> for Worker {
    fn handle(&mut self, event: i32) -> Handled {
        self.oport.trigger((self.fun)(event));
        Handled::Ok
    }
}

kompact::prelude::ignore_requests!(WorkerPort, Manager);

impl Require<WorkerPort> for Manager {
    fn handle(&mut self, event: i32) -> Handled {
        info!(self.log(), "Received output data {}", event);
        Handled::Ok
    }
}

impl Driver {
    fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            managers: Vec::new(),
        }
    }
}

impl Manager {
    fn new(safe_mode: bool, driver: ActorPath) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            driver,
            safe_mode,
            workers: Vec::new(),
            iport: RequiredPort::uninitialised(),
            oport: ProvidedPort::uninitialised(),
        }
    }
}

/// Program Lookup Table
#[derive(Debug, Deserialize, Serialize)]
enum PLT {
    UDF0,
    UDF1,
}

/// These are what we want to transfer over the wire.
fn udf0(a: i32) -> i32 {
    a + 1
}

fn udf1(a: i32) -> i32 {
    a * 2
}

impl Driver {
    fn handle_timeout(&mut self, _timer: ScheduledTimer) -> Handled {
        for manager in &self.managers {
            for i in 0..10 {
                manager.tell((ManagerMessage::Input(i), Serde), self);
            }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for Driver {
    fn on_start(&mut self) -> Handled {
        self.schedule_periodic(
            Duration::new(1, 0),
            Duration::new(1, 0),
            Driver::handle_timeout,
        );
        Handled::Ok
    }
}

kompact::ignore_lifecycle!(Worker);

impl ComponentLifecycle for Manager {
    fn on_start(&mut self) -> Handled {
        if self.safe_mode {
            self.driver
                .tell((DriverMessage::RegisterSafely, Serde), self);
        } else {
            self.driver
                .tell((DriverMessage::RegisterUnsafely, Serde), self);
        }
        Handled::Ok
    }
}

fn socket(port: &str) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port.parse().unwrap())
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap();
    let role = args.next().unwrap();
    let driver_port = args.next().unwrap();
    let manager_port = args.next().unwrap();

    let mut cfg = KompactConfig::default();

    let driver_path = "driver".to_string();

    let driver_socket = socket(&driver_port);
    let manager_socket = socket(&manager_port);

    match role.as_str() {
        "driver" => {
            cfg.system_components(
                DeadletterBox::new,
                NetworkConfig::new(driver_socket).build(),
            );
            let sys = cfg.build().expect("KompactSystem");
            let (driver, path) = sys.create_and_register(Driver::new);
            sys.register_by_alias(&driver, driver_path).wait().unwrap();
            path.wait().unwrap();
            sys.start(&driver);
            sys.await_termination();
        }
        "manager" => {
            cfg.system_components(
                DeadletterBox::new,
                NetworkConfig::new(manager_socket).build(),
            );
            let sys = cfg.build().expect("KompactSystem");
            let driver_path = ActorPath::Named(NamedPath::with_socket(
                Transport::Tcp,
                driver_socket,
                vec![driver_path],
            ));
            let (manager, path) = sys.create_and_register(move || Manager::new(false, driver_path));
            path.wait().unwrap();
            sys.start(&manager);
            sys.await_termination();
        }
        _ => unreachable!(),
    }
}
