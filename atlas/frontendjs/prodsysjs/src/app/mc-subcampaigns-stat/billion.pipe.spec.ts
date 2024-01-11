import { BillionPipe } from './billion.pipe';

describe('BillionPipe', () => {
  it('create an instance', () => {
    const pipe = new BillionPipe();
    expect(pipe).toBeTruthy();
  });
});
