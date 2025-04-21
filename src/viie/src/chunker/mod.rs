pub trait Chunkable {
  type Input;
  type Output;

  fn chunk(input: Self::Input) -> Result<Vec<Self::Output>>;
}