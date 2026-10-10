use crate::seq_nr::{SeqNr, SEQ_NR_MAX};

// T-9: 上限は 2^53 − 1 = 9007199254740991。
#[test]
fn should_seq_nr_max_equal_two_to_the_fifty_third_minus_one() {
  assert_eq!(SEQ_NR_MAX, 9007199254740991);
  assert_eq!(SEQ_NR_MAX, (1u64 << 53) - 1);
}

// T-9: `SeqNr` は符号なし 64 ビットで、負の値を表せない。
#[test]
fn should_seq_nr_be_unsigned_sixty_four_bits() {
  let _: SeqNr = u64::MAX;
  assert_eq!(SeqNr::default(), 0);
}
