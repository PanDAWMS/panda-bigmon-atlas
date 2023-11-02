export function uniqByForEach<T>(array: T[]) {
    const result: T[] = [];
    array.forEach((item) => {
        if (!result.includes(item)) {
            result.push(item);
        }
    })
    return result;
}
