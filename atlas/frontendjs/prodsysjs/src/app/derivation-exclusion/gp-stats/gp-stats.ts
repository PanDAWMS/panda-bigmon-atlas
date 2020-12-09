export interface GroupProductionStats {
    id: number;
    ami_tag: string;
    output_format: string;
    real_data: boolean;
    size: number;
    containers: number;
    to_delete_containers: number;
    to_delete_size: number;
    timestamp: string;
}
