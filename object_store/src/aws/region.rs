pub trait RegionProvider {
    fn get_region(&self) -> Option<String>;
}
