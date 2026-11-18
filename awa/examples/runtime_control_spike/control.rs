use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct LocalHalt {
    pub token: u64,
    pub fleet_generation: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct Controls {
    pub startup_allowed: bool,
    pub feature_allowed: bool,
    pub control_fresh: bool,
    pub generation: u64,
    pub halted: bool,
    pub local_halt: Option<LocalHalt>,
    #[serde(skip)]
    next_local_token: u64,
}

impl Default for Controls {
    fn default() -> Self {
        Self {
            startup_allowed: true,
            feature_allowed: true,
            control_fresh: true,
            generation: 0,
            halted: false,
            local_halt: None,
            next_local_token: 0,
        }
    }
}

impl Controls {
    pub fn allows_admission(&self) -> bool {
        self.startup_allowed
            && self.feature_allowed
            && self.control_fresh
            && !self.halted
            && self.local_halt.is_none()
    }

    pub fn halt(&mut self) {
        self.generation += 1;
        self.halted = true;
    }

    pub fn resume(&mut self, expected_generation: u64) -> bool {
        if expected_generation != self.generation || !self.halted {
            return false;
        }
        self.halted = false;
        if self.local_halt.as_ref().is_some_and(|local| {
            local
                .fleet_generation
                .is_some_and(|generation| generation <= expected_generation)
        }) {
            self.local_halt = None;
        }
        self.generation += 1;
        true
    }

    pub fn halt_local(&mut self) -> u64 {
        self.next_local_token += 1;
        self.local_halt = Some(LocalHalt {
            token: self.next_local_token,
            fleet_generation: None,
        });
        self.next_local_token
    }

    pub fn associate_local(&mut self) {
        if !self.halted {
            return;
        }
        if let Some(local) = &mut self.local_halt {
            local.fleet_generation = Some(self.generation);
        }
    }

    pub fn resume_local(&mut self, expected_token: u64) -> bool {
        if self
            .local_halt
            .as_ref()
            .is_some_and(|local| local.token == expected_token)
        {
            self.local_halt = None;
            true
        } else {
            false
        }
    }
}
