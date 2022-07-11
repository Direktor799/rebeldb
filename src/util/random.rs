const M: u32 = 0x7fffffff;
const A: u64 = 16807;

pub struct Random {
    seed: u32,
}

impl Random {
    pub fn new(seed: u32) -> Self {
        let mut seed = seed & M;
        if seed == 0 || seed == M {
            seed = 1;
        }
        Self { seed }
    }

    pub fn next(&mut self) -> u32 {
        let product = self.seed as u64 * A;
        // product % M
        self.seed = ((product >> 31) + (product & M as u64)) as u32;
        if self.seed > M {
            self.seed -= M;
        }
        self.seed
    }

    pub fn uniform(&mut self, n: u32) -> u32 {
        self.next() % n
    }

    pub fn one_in(&mut self, n: u32) -> bool {
        (self.next() % n) == 0
    }

    pub fn skewed(&mut self, max_log: u32) -> u32 {
        let tmp = 1 << self.uniform(max_log + 1);
        self.uniform(tmp)
    }
}
