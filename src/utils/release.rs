pub struct DropRelease<T: Fn()> {
    func: T,
}

impl<T: Fn()> Drop for DropRelease<T> {
    fn drop(&mut self) {
        (self.func)();
    }
}

impl<T: Fn()> DropRelease<T> {
    pub fn new(func: T) -> Self {
        DropRelease { func: func }
    }
}
