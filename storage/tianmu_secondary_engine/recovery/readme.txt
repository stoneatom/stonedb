1: Fast Recovery
  IMCS data will flush out to disk periodicaly. when instance start will just only need to reload the data
  from disk to imcs.

  it reduces the time to populate data into the IM column store when a database instance restarts. IM FastStart 
  achieves this by periodically saving a copy of the data currently populated in the IM column store on the disk 
  in its compressed columnar format.

