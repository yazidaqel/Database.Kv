namespace FraudDetector.Database.Kv.Engine.Page
{
    internal class PageLocation
    {
        public bool IsPersisted { get; set; }
        public string PersistenceFileLocation { get; set; }
        public int Position { get; set; }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;

            if (!(obj is PageLocation))
                return false;

            var value = (PageLocation)obj;

            if (value.IsPersisted == IsPersisted && value.PersistenceFileLocation == PersistenceFileLocation && value.Position == Position)
                return true;

            return false;
        }

        public override int GetHashCode()
        {
            return IsPersisted.GetHashCode() + PersistenceFileLocation.GetHashCode() + Position.GetHashCode();
        }

    }
}
