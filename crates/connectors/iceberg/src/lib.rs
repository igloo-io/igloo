// Placeholder for the igloo-iceberg-connector library.
// This crate will provide an Iceberg table implementation for the Igloo query engine.

// Modules will be defined here, e.g.:
// mod table;
// mod schema;
// mod catalog;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
