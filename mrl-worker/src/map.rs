use crate::core::MapJobRequest;

pub async fn perform_map(request: MapJobRequest) {
    let input_path = request.input_files;
    let workload = request.workload;
    let url = request.presigned_url;
    let aux = request.aux;
    
    
    println!("hello i'm map");
}
