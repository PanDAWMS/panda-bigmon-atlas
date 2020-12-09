import { RucioURLPipe } from './rucio-url.pipe';

describe('RucioURLPipe', () => {
  it('create an instance', () => {
    const pipe = new RucioURLPipe();
    expect(pipe).toBeTruthy();
  });
});
